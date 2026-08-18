// Package gateway implements a forward HTTP proxy that authenticates clients
// against scheduling groups and forwards traffic via a live proxy from the
// matched group. Group-level failover (primary -> backups) is transparent to
// the client: every request picks a fresh proxy from the current usable tier.
package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
)

// dialTimeout bounds connecting to the upstream proxy.
const dialTimeout = 15 * time.Second

// GroupSource returns the current group definitions. The gateway reads this
// on every request so runtime group CRUD takes effect immediately.
type GroupSource func() []config.GroupCfg

// Gateway is a forward HTTP proxy bound to a scheduling group.
type Gateway struct {
	pool      *pool.Pool
	groupsSrc GroupSource
	logger    *slog.Logger
	credToGrp atomic.Value // map[string]string key "user:pass" -> group name
	hc        *http.Client
}

// New creates a gateway serving the pool with dynamic group credentials.
func New(p *pool.Pool, groupsSrc GroupSource, logger *slog.Logger) *Gateway {
	g := &Gateway{
		pool:      p,
		groupsSrc: groupsSrc,
		logger:    logger,
		hc: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   32,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   15 * time.Second,
				ExpectContinueTimeout: time.Second,
			},
		},
	}
	g.rebuildCreds()
	return g
}

// Rebuild refreshes the in-memory credential map from the group source.
func (g *Gateway) Rebuild() {
	g.rebuildCreds()
}

func (g *Gateway) rebuildCreds() {
	m := map[string]string{}
	if g.groupsSrc != nil {
		for _, c := range g.groupsSrc() {
			// Credentials must be configured as a pair: a group with only a
			// username (or only a password) is not registered for gateway
			// authentication.
			if c.Username != "" && c.Password != "" {
				m[credKey(c.Username, c.Password)] = c.Name
			}
		}
	}
	g.credToGrp.Store(m)
}

func credKey(user, pass string) string {
	return user + "\x00" + pass
}

// LookupGroup resolves credentials to a group name.
func (g *Gateway) LookupGroup(user, pass string) string {
	m, _ := g.credToGrp.Load().(map[string]string)
	return m[credKey(user, pass)]
}

// Serve listens and serves the proxy gateway until ctx is cancelled.
func (g *Gateway) Serve(ctx context.Context, listen string) error {
	srv := &http.Server{
		Addr:              listen,
		Handler:           g,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		g.logger.Info("proxy gateway started", "listen", listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errc:
		return err
	}
}

// ServeHTTP implements http.Handler.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := proxyBasicAuth(r)
	if !ok {
		g.requireAuth(w)
		return
	}
	group := g.LookupGroup(user, pass)
	if group == "" {
		g.logger.Warn("gateway auth failed", "user", user)
		g.requireAuth(w)
		return
	}
	// Special direct-connect endpoint: for pure-tunnel groups it hands back the
	// upstream tunnel address so clients can connect straight to the tunnel
	// provider, keeping data traffic off this host (and off its metered
	// bandwidth). Only the URL path triggers it; regular proxy requests are
	// unaffected.
	if r.Method == http.MethodGet && r.URL.Path == "/direct" {
		g.handleDirect(w, r, group)
		return
	}
	if r.Method == http.MethodConnect {
		g.handleConnect(w, r, group)
		return
	}
	g.handleHTTP(w, r, group)
}

// findGroup returns the group config for a name.
func (g *Gateway) findGroup(name string) (config.GroupCfg, bool) {
	if g.groupsSrc == nil {
		return config.GroupCfg{}, false
	}
	for _, c := range g.groupsSrc() {
		if c.Name == name {
			return c, true
		}
	}
	return config.GroupCfg{}, false
}

// handleDirect serves the direct-connect endpoint. When every provider
// referenced by the group is a tunnel with a single shared upstream address,
// it returns that address so clients can bypass this host entirely. Otherwise
// it explains why direct connection is not possible.
func (g *Gateway) handleDirect(w http.ResponseWriter, r *http.Request, group string) {
	cfg, ok := g.findGroup(group)
	if !ok {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}
	provSet := map[string]bool{}
	for _, pn := range cfg.Primary {
		provSet[pn] = true
	}
	for _, b := range cfg.Backups {
		for _, pn := range b.Providers {
			provSet[pn] = true
		}
	}

	var upstream *model.Proxy
	multi := false
	for _, pr := range g.pool.All() {
		if !provSet[pr.Provider] {
			continue
		}
		if pr.Kind != model.KindTunnel {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"direct":null,"reason":"group contains non-tunnel providers; traffic must be relayed"}` + "\n"))
			return
		}
		if upstream == nil {
			upstream = pr
		} else if upstream.Host != pr.Host || upstream.Port != pr.Port {
			multi = true
		}
	}
	if upstream == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"direct":null,"reason":"no tunnel provider available in group"}` + "\n"))
		return
	}
	if multi {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"direct":null,"reason":"group references multiple tunnel upstreams; use the relay gateway instead"}` + "\n"))
		return
	}
	scheme := upstream.Scheme
	if scheme == "" {
		scheme = "http"
	}
	hostPort := net.JoinHostPort(upstream.Host, strconv.Itoa(upstream.Port))
	direct := scheme + "://" + hostPort
	if upstream.Username != "" {
		direct = scheme + "://" + url.UserPassword(upstream.Username, upstream.Password).String() + "@" + hostPort
	}
	g.logger.Info("gateway direct issued", "group", group, "direct", hostPort)
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"direct":%s}`+"\n", strconv.Quote(direct))
}

// proxyBasicAuth reads credentials from the Proxy-Authorization header
// (standard for HTTP proxy clients; r.BasicAuth only reads Authorization).
func proxyBasicAuth(r *http.Request) (user, pass string, ok bool) {
	h := r.Header.Get("Proxy-Authorization")
	if !strings.HasPrefix(h, "Basic ") {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(h, "Basic "))
	if err != nil {
		return "", "", false
	}
	user, pass, ok = strings.Cut(string(raw), ":")
	return user, pass, ok
}

func (g *Gateway) requireAuth(w http.ResponseWriter) {
	w.Header().Set("Proxy-Authenticate", `Basic realm="proxy-pool"`)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusProxyAuthRequired)
	_, _ = w.Write([]byte("proxy authentication required\n"))
}

// pickProxy selects one live proxy from the group's current usable tier.
func (g *Gateway) pickProxy(group string) *model.Proxy {
	return g.pool.NextInGroup(group)
}

// handleHTTP forwards a non-CONNECT request through the selected upstream.
func (g *Gateway) handleHTTP(w http.ResponseWriter, r *http.Request, group string) {
	pr := g.pickProxy(group)
	if pr == nil {
		g.noProxy(w, group)
		return
	}
	upstream, err := g.dialUpstream(pr, r.URL)
	if err != nil {
		g.logger.Warn("gateway upstream dial failed", "group", group, "err", err)
		http.Error(w, "upstream proxy unreachable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	// Clone the request with hop-by-hop headers removed.
	out := r.Clone(r.Context())
	out.RequestURI = ""
	removeHopHeaders(out.Header)
	out.Header.Del("Proxy-Authorization")
	out.Header.Del("Proxy-Connection")

	if err := out.Write(upstream); err != nil {
		g.logger.Warn("gateway forward failed", "group", group, "err", err)
		return
	}
	resp, err := http.ReadResponse(bufio.NewReader(upstream), out)
	if err != nil {
		g.logger.Warn("gateway read response failed", "group", group, "err", err)
		http.Error(w, "upstream proxy error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleConnect opens a CONNECT tunnel through the selected upstream.
func (g *Gateway) handleConnect(w http.ResponseWriter, r *http.Request, group string) {
	pr := g.pickProxy(group)
	if pr == nil {
		g.noProxy(w, group)
		return
	}
	upstream, err := dialTCP(pr, dialTimeout)
	if err != nil {
		g.logger.Warn("gateway connect dial failed", "group", group, "err", err)
		http.Error(w, "upstream proxy unreachable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: r.Host},
		Host:   r.Host,
		Header: make(http.Header),
	}
	if pr.Username != "" {
		req.Header.Set("Proxy-Authorization", basicAuth(pr.Username, pr.Password))
	}
	if err := req.Write(upstream); err != nil {
		http.Error(w, "upstream connect failed", http.StatusBadGateway)
		return
	}
	resp, err := http.ReadResponse(bufio.NewReader(upstream), req)
	if err != nil {
		g.logger.Warn("gateway connect response failed", "group", group, "err", err)
		http.Error(w, "upstream connect failed", http.StatusBadGateway)
		return
	}
	if resp.StatusCode != http.StatusOK {
		g.logger.Warn("gateway connect rejected", "group", group, "status", resp.StatusCode)
		http.Error(w, "upstream connect rejected", http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	client, rw, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return
	}
	defer client.Close()
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}
	// Bidirectional copy: client <-> upstream tunnel.
	go func() {
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(client, upstream)
}

// ForwardPlain proxies a plain HTTP GET to target through a live proxy from
// the group, returning status, headers and body. Used by the web server's
// path-based gateway endpoint (/api/v1/gw) so the gateway stays usable behind
// preview reverse proxies that only forward plain path requests.
func (g *Gateway) ForwardPlain(target *url.URL, group string) (int, http.Header, []byte, error) {
	pr := g.pickProxy(group)
	if pr == nil {
		return 0, nil, nil, errors.New("no available proxy in group: " + group)
	}
	upstream, err := g.dialUpstream(pr, target)
	if err != nil {
		return 0, nil, nil, err
	}
	defer upstream.Close()

	out := &http.Request{
		Method: http.MethodGet,
		URL:    target,
		Host:   target.Host,
		Header: make(http.Header),
	}
	out.Header.Set("User-Agent", "proxy-pool-gateway/1.0")
	if err := out.Write(upstream); err != nil {
		return 0, nil, nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(upstream), out)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20)) // cap at 4 MiB
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// noProxy responds when the group has no usable proxy right now.
func (g *Gateway) noProxy(w http.ResponseWriter, group string) {
	g.logger.Warn("gateway no usable proxy", "group", group)
	http.Error(w, "no available proxy in group: "+group, http.StatusServiceUnavailable)
}

// dialUpstream connects to the upstream proxy and returns a conn already
// wrapped for the given URL. It supports http scheme; https upstreams are
// dialed with a TLS handshake on the proxy port.
func (g *Gateway) dialUpstream(pr *model.Proxy, target *url.URL) (net.Conn, error) {
	conn, err := dialTCP(pr, dialTimeout)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(pr.Scheme, "https") {
		tlsConn := tlsClient(conn, pr.Host)
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return conn, nil
}

func dialTCP(pr *model.Proxy, timeout time.Duration) (net.Conn, error) {
	addr := net.JoinHostPort(pr.Host, fmt.Sprintf("%d", pr.Port))
	return net.DialTimeout("tcp", addr, timeout)
}

func tlsClient(conn net.Conn, serverName string) *tls.Conn {
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	return tls.Client(conn, cfg)
}

func basicAuth(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func removeHopHeaders(h http.Header) {
	// Also drop headers nominated by the Connection header.
	for _, token := range h.Values("Connection") {
		for _, name := range strings.Split(token, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, hh := range hopHeaders {
		h.Del(hh)
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
