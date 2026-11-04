package provider

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

// StaticURLs is a provider built from a list of directly-pasted proxy URLs.
// It serves both tunnel and sticky providers: each URL becomes one proxy, and
// the whole list participates in the pool (weighted scheduling). It is the
// primary way to onboard tunnel accounts and sticky (long-lived fixed IP)
// tunnels without splitting gateway / port / credentials into separate fields.
type StaticURLs struct {
	cfg config.ProviderCfg
}

func NewStaticURLs(cfg config.ProviderCfg) *StaticURLs {
	return &StaticURLs{cfg: cfg}
}

func (s *StaticURLs) Name() string { return s.cfg.Name }

func (s *StaticURLs) Kind() model.Kind {
	if s.cfg.Type == "sticky" {
		return model.KindSticky
	}
	return model.KindTunnel
}

func (s *StaticURLs) Weight() int32 { return int32(s.cfg.Weight) }

func (s *StaticURLs) CheckURL() string { return s.cfg.CheckURL }

func (s *StaticURLs) Initial(ctx context.Context) ([]*model.Proxy, error) {
	return s.Refresh(ctx)
}

// Refresh builds one proxy per URL. URLs that fail to parse are skipped; the
// rest share the provider's weight. Sticky / tunnel URLs never expire (they are
// long-lived endpoints), so ExpireAt stays 0.
func (s *StaticURLs) Refresh(_ context.Context) ([]*model.Proxy, error) {
	proxies := make([]*model.Proxy, 0, len(s.cfg.URLs))
	seen := make(map[string]struct{}, len(s.cfg.URLs))
	for _, raw := range s.cfg.URLs {
		scheme, host, port, username, password, ok := ParseProxyURL(raw)
		if !ok {
			continue
		}
		id := fmt.Sprintf("%s:%s:%d", s.cfg.Name, host, port)
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		pr := &model.Proxy{
			ID:              id,
			Provider:        s.cfg.Name,
			Kind:            s.Kind(),
			Scheme:          scheme,
			Host:            host,
			Port:            port,
			Username:        username,
			Password:        password,
			Weight:          int32(s.cfg.Weight),
			CheckURL:        s.CheckURL(),
			CheckURLs:       s.cfg.EnabledCheckURLs(),
			CheckIntervalMS: int64(s.cfg.CheckIntervalS) * 1000,
			Priority:        s.cfg.Priority,
			MinAliveRatio:   s.cfg.MinAliveRatio,
			StickySeconds:   s.cfg.StickySeconds,
		}
		pr.Alive.Store(true)
		proxies = append(proxies, pr)
	}
	return proxies, nil
}

// ParseProxyURL parses a single proxy URL of the form
// `[scheme://][user:pass@]host:port`. When the scheme is omitted it defaults to
// http. It returns the parsed scheme (http / https / socks5), host, port and
// credentials, or ok=false when the string cannot be parsed into a valid proxy.
func ParseProxyURL(raw string) (scheme, host string, port int, username, password string, ok bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return
	}
	// 无协议开头（http/https/socks）时默认 http。
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return
	}
	if u.Host == "" {
		return
	}
	scheme = strings.ToLower(u.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "https" && scheme != "socks5" && scheme != "socks5h" {
		scheme = "http"
	}
	host = u.Hostname()
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	if portStr := u.Port(); portStr != "" {
		port, err = strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return "", "", 0, "", "", false
		}
	} else if scheme == "https" {
		port = 443
	} else {
		port = 80
	}
	if host == "" {
		return
	}
	return scheme, host, port, username, password, true
}
