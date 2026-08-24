package gateway

import (
	"encoding/base64"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
)

// fakeProxyServer is a minimal HTTP forward proxy that forwards requests to a
// fixed target, so we can verify the gateway forwards via the selected proxy.
func fakeUpstream(t *testing.T, target string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip the absolute-form: rebuild URL path.
		path := r.URL.Path
		if path == "" {
			path = "/"
		}
		http.Redirect(w, r, target+path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(s.Close)
	return s
}

// fakeTarget echoes the request method and path.
func fakeTarget(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Method+" "+r.URL.Path)
	}))
	t.Cleanup(s.Close)
	return s
}

func addProxy(p *pool.Pool, host string, port int) {
	pr := &model.Proxy{ID: host + ":" + "8080", Provider: "p1", Kind: model.KindIPPool, Scheme: "http", Host: host, Port: port}
	pr.Alive.Store(true)
	p.Add(pr)
}

// addProxyID adds a live proxy with an explicit unique ID (used when a test
// needs multiple upstreams on the same host).
func addProxyID(p *pool.Pool, id string, host string, port int) {
	pr := &model.Proxy{ID: id, Provider: "p1", Kind: model.KindIPPool, Scheme: "http", Host: host, Port: port}
	pr.Alive.Store(true)
	p.Add(pr)
}

func groups() []config.GroupCfg {
	return []config.GroupCfg{
		{Name: "g1", Type: "static", Primary: []string{"p1"}, Username: "user1", Password: "pass1"},
		{Name: "g2", Type: "free", Primary: []string{"p1"}, Username: "user2", Password: "pass2"},
	}
}

func newTestGateway(p *pool.Pool, src GroupSource) *Gateway {
	return New(p, src, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func basicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func TestAuthRequired(t *testing.T) {
	p := pool.NewPool()
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/proxy")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("want 407, got %d", resp.StatusCode)
	}
}

func TestAuthInvalid(t *testing.T) {
	p := pool.NewPool()
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/proxy", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("user1", "wrong"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("want 407, got %d", resp.StatusCode)
	}
}

func TestAuthRequiresPasswordPair(t *testing.T) {
	p := pool.NewPool()
	src := func() []config.GroupCfg {
		return []config.GroupCfg{
			{Name: "g-user-only", Type: "static", Primary: []string{"p1"}, Username: "onlyuser", Password: ""},
			{Name: "g-pass-only", Type: "static", Primary: []string{"p1"}, Username: "", Password: "onlypass"},
			{Name: "g-empty", Type: "static", Primary: []string{"p1"}, Username: "", Password: ""},
			{Name: "g-valid", Type: "static", Primary: []string{"p1"}, Username: "u", Password: "p"},
		}
	}
	g := newTestGateway(p, src)
	if got := g.LookupGroup("onlyuser", ""); got != "" {
		t.Fatalf("username-only group must not authenticate, got %q", got)
	}
	if got := g.LookupGroup("", "onlypass"); got != "" {
		t.Fatalf("password-only group must not authenticate, got %q", got)
	}
	if got := g.LookupGroup("u", "p"); got != "g-valid" {
		t.Fatalf("valid pair should resolve to g-valid, got %q", got)
	}
}

func TestAuthOKNoProxy(t *testing.T) {
	p := pool.NewPool() // empty pool
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/some/path", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("user1", "pass1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", resp.StatusCode)
	}
}

// squidErrorProxy is a fake upstream that answers every request with a Squid
// ERR_INVALID_URL error page, mimicking misbehaving free proxies.
func squidErrorProxy(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "squid/4.15")
		w.Header().Set("Content-Type", "text/html;charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "<!DOCTYPE html><html><head><title>ERROR: The requested URL could not be retrieved</title></head><body id=ERR_INVALID_URL></body></html>")
	}))
	t.Cleanup(s.Close)
	return s
}

func TestHTTPForwardSkipsSquidErrorPage(t *testing.T) {
	// First-added proxy is the broken Squid one; the good upstream must be
	// tried next thanks to the gateway's retry logic.
	bad := squidErrorProxy(t)
	badHost, badPort := hostPort(t, bad.Listener.Addr())
	target := fakeTarget(t)
	good := fakeUpstream(t, target.URL)
	goodHost, goodPort := hostPort(t, good.Listener.Addr())

	p := pool.NewPool()
	p.SetGroups(groups())
	addProxyID(p, "bad", badHost, badPort)
	addProxyID(p, "good", goodHost, goodPort)
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/hello", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("user1", "pass1"))
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("want 307 redirect from good upstream, got %d body=%s", resp.StatusCode, body)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasSuffix(loc, "/hello") {
		t.Fatalf("unexpected Location %q", loc)
	}
}

func TestHTTPForwardAllUpstreamsFail(t *testing.T) {
	bad1 := squidErrorProxy(t)
	h1, p1 := hostPort(t, bad1.Listener.Addr())
	bad2 := squidErrorProxy(t)
	h2, p2 := hostPort(t, bad2.Listener.Addr())

	p := pool.NewPool()
	p.SetGroups(groups())
	addProxyID(p, "bad1", h1, p1)
	addProxyID(p, "bad2", h2, p2)
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/hello", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("user1", "pass1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("want 502 after exhausting bad proxies, got %d", resp.StatusCode)
	}
}

func TestHTTPForward(t *testing.T) {
	target := fakeTarget(t)
	upstream := fakeUpstream(t, target.URL)
	host, port := hostPort(t, upstream.Listener.Addr())

	p := pool.NewPool()
	p.SetGroups(groups())
	addProxy(p, host, port)
	g := newTestGateway(p, groups)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/hello", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("user1", "pass1"))
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("want 307 redirect from upstream proxy, got %d body=%s", resp.StatusCode, body)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasSuffix(loc, "/hello") {
		t.Fatalf("unexpected Location %q", loc)
	}
}

func hostPort(t *testing.T, addr net.Addr) (string, int) {
	t.Helper()
	h, p, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Trim(h, "[]"), port
}

// TestDirectTunnel returns the single shared upstream for a pure-tunnel group.
func TestDirectTunnel(t *testing.T) {
	p := pool.NewPool()
	p.SetGroups(groups())
	pr := &model.Proxy{ID: "tun1", Provider: "p-tunnel", Kind: model.KindTunnel, Scheme: "http", Host: "tunnel.example.com", Port: 3128, Username: "upuser", Password: "uppass"}
	pr.Alive.Store(true)
	p.Add(pr)
	src := func() []config.GroupCfg {
		return []config.GroupCfg{
			{Name: "tgroup", Type: "static", Primary: []string{"p-tunnel"}, Username: "tu", Password: "tp"},
		}
	}
	g := newTestGateway(p, src)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/direct", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("tu", "tp"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"direct":"http://upuser:uppass@tunnel.example.com:3128"`) {
		t.Fatalf("want direct tunnel address in response, got %s", body)
	}
}

// TestDirectRejectsMixed returns direct:null when the group mixes tunnel and
// non-tunnel providers, since those must keep being relayed.
func TestDirectRejectsMixed(t *testing.T) {
	p := pool.NewPool()
	p.SetGroups(groups())
	tun := &model.Proxy{ID: "tun1", Provider: "p-tunnel", Kind: model.KindTunnel, Scheme: "http", Host: "tunnel.example.com", Port: 3128}
	tun.Alive.Store(true)
	p.Add(tun)
	ipp := &model.Proxy{ID: "ipp1", Provider: "p-ippool", Kind: model.KindIPPool, Scheme: "http", Host: "1.2.3.4", Port: 8080}
	ipp.Alive.Store(true)
	p.Add(ipp)
	src := func() []config.GroupCfg {
		return []config.GroupCfg{
			{Name: "mg", Type: "static", Primary: []string{"p-ippool"}, Backups: []config.BackupPool{{Name: "b", Providers: []string{"p-tunnel"}}}, Username: "mu", Password: "mp"},
		}
	}
	g := newTestGateway(p, src)
	srv := httptest.NewServer(g)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/direct", nil)
	req.Header.Set("Proxy-Authorization", basicAuthHeader("mu", "mp"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"direct":null`) {
		t.Fatalf("want direct:null for mixed group, got %s", body)
	}
}
