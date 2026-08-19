package health

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
)

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// proxyTarget is a fake target that records how many times it was hit via proxy.
type proxyTarget struct {
	hits atomic.Int32
}

func (p *proxyTarget) handler(w http.ResponseWriter, r *http.Request) {
	p.hits.Add(1)
	w.WriteHeader(http.StatusOK)
}

// fakeProxy is a minimal HTTP forward proxy that forwards to upstream.
func fakeProxy(upstream string, fail *atomic.Bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if fail != nil && fail.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		resp, err := http.Get(upstream)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func startProxyServer(h http.Handler) *httptest.Server {
	srv := httptest.NewServer(h)
	return srv
}

func TestCheckerMarksDeadAndRecovers(t *testing.T) {
	// upstream target
	target := &proxyTarget{}
	ts := httptest.NewServer(http.HandlerFunc(target.handler))
	defer ts.Close()

	var fail atomic.Bool
	proxySrv := startProxyServer(fakeProxy(ts.URL, &fail))
	defer proxySrv.Close()

	// strip scheme from proxy addr
	proxyURL := proxySrv.URL // http://127.0.0.1:port
	host, portStr := splitHostPort(proxyURL[len("http://"):])

	p := pool.NewPool()
	pr := &model.Proxy{
		ID:       "t1",
		Provider: "test",
		Kind:     model.KindTunnel,
		Scheme:   "http",
		Host:     host,
		Port:     atoi(portStr),
		Weight:   1,
		CheckURL: ts.URL,
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{
		IntervalSeconds: 1,
		TimeoutMs:       2000,
		Concurrency:     8,
		MaxFails:        3,
	}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// healthy: proxy stays alive
	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should be alive while healthy")
	}

	// outage: mark proxy failed until it goes dead
	fail.Store(true)
	for range 4 {
		c.checkAll(ctx)
	}
	if pr.Alive.Load() {
		t.Fatal("proxy should be dead after repeated failures")
	}

	// recover: proxy becomes alive again
	fail.Store(false)
	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should recover after health returns")
	}
}

func TestCheckerSkipsWhenNoProxies(t *testing.T) {
	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 1000, Concurrency: 4, MaxFails: 3}
	c := NewChecker(cfg, pool.NewPool(), newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.checkAll(ctx) // should not panic
}

func TestCheckerFreeProxyDeletedOnFailure(t *testing.T) {
	target := &proxyTarget{}
	ts := httptest.NewServer(http.HandlerFunc(target.handler))
	defer ts.Close()

	var fail atomic.Bool
	proxySrv := startProxyServer(fakeProxy(ts.URL, &fail))
	defer proxySrv.Close()

	host, portStr := splitHostPort(proxySrv.URL[len("http://"):])
	p := pool.NewPool()
	pr := &model.Proxy{
		ID:              "free1",
		Provider:        "freeproxy",
		Kind:            model.KindIPPool,
		Scheme:          "http",
		Host:            host,
		Port:            atoi(portStr),
		Weight:          1,
		CheckURL:        ts.URL,
		Free:            true,
		DeleteLatencyMS: 5000,
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 2000, Concurrency: 8, MaxFails: 3}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// healthy: stays, and stays in pool
	c.checkAll(ctx)
	if p.Get("free1") == nil {
		t.Fatal("healthy free proxy should remain in pool")
	}

	// one failure -> immediately removed, not just marked dead
	fail.Store(true)
	c.checkAll(ctx)
	if p.Get("free1") != nil {
		t.Fatal("free proxy should be removed from pool after single failure")
	}
}

func TestCheckerFreeProxyDeletedOnHighLatency(t *testing.T) {
	// upstream that accepts the CONNECT but sleeps longer than the threshold
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	host, portStr := splitHostPort(slow.URL[len("http://"):])
	p := pool.NewPool()
	pr := &model.Proxy{
		ID:              "free2",
		Provider:        "freeproxy",
		Kind:            model.KindIPPool,
		Scheme:          "http",
		Host:            host,
		Port:            atoi(portStr),
		Weight:          1,
		CheckURL:        slow.URL,
		Free:            true,
		DeleteLatencyMS: 100, // 0.1s threshold: slow probe exceeds it
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 5000, Concurrency: 8, MaxFails: 3}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.checkAll(ctx)
	if p.Get("free2") != nil {
		t.Fatal("slow free proxy should be removed from pool when latency exceeds threshold")
	}
}

func TestCheckerAliveWhenAnyCheckURLReachable(t *testing.T) {
	// upstream targets: only the second one works through the failing proxy
	deadTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log("dead target hit (unexpected)")
	}))
	defer deadTarget.Close()

	target := &proxyTarget{}
	ts := httptest.NewServer(http.HandlerFunc(target.handler))
	defer ts.Close()

	proxySrv := startProxyServer(fakeProxy(ts.URL, nil))
	defer proxySrv.Close()
	host, portStr := splitHostPort(proxySrv.URL[len("http://"):])

	p := pool.NewPool()
	pr := &model.Proxy{
		ID:       "multi1",
		Provider: "test",
		Kind:     model.KindIPPool,
		Scheme:   "http",
		Host:     host,
		Port:     atoi(portStr),
		Weight:   1,
		CheckURLs: []string{
			"http://127.0.0.1:1/", // unreachable
			ts.URL,                // reachable
		},
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 2000, Concurrency: 8, MaxFails: 3}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should stay alive when any of its check URLs is reachable")
	}
}

func TestCheckerFallsBackToGlobalDefaultCheckURLs(t *testing.T) {
	// proxy with no provider-specific URL: falls back to global defaults
	target := &proxyTarget{}
	ts := httptest.NewServer(http.HandlerFunc(target.handler))
	defer ts.Close()

	proxySrv := startProxyServer(fakeProxy(ts.URL, nil))
	defer proxySrv.Close()
	host, portStr := splitHostPort(proxySrv.URL[len("http://"):])

	p := pool.NewPool()
	pr := &model.Proxy{
		ID:       "global1",
		Provider: "test",
		Kind:     model.KindIPPool,
		Scheme:   "http",
		Host:     host,
		Port:     atoi(portStr),
		Weight:   1,
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{
		IntervalSeconds: 1,
		TimeoutMs:       2000,
		Concurrency:     8,
		MaxFails:        3,
		CheckURLs: []config.CheckURLItem{
			{Name: "unreachable", URL: "http://127.0.0.1:1/"},
			{Name: "reachable", URL: ts.URL},
		},
	}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should stay alive via global default check URLs")
	}
}

func TestParseCountry(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"plain country code", "HK\n", "HK"},
		{"plain country code no newline", "US", "US"},
		{"json countryCode", `{"countryCode":"HK","query":"1.2.3.4"}` + "\n", "HK"},
		{"json with other fields", `{"status":"success","country":"Hong Kong","countryCode":"HK","query":"1.2.3.4"}` + "\n", "HK"},
		{"bare ip", "103.156.242.197", ""},
		{"bare ip newline", "203.198.248.244\n", ""},
		{"empty", "", ""},
		{"json no countryCode", `{"query":"1.2.3.4"}`, ""},
		{"lowercase code", "hk", "HK"},
		{"too long", "HKG", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseCountry([]byte(c.body)); got != c.want {
				t.Fatalf("parseCountry(%q) = %q, want %q", c.body, got, c.want)
			}
		})
	}
}

func TestCheckerDetectsCountryFromCheckURL(t *testing.T) {
	// upstream that returns a country code (simulates ip-api.com/line)
	cc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("HK"))
	}))
	defer cc.Close()

	proxySrv := startProxyServer(fakeProxy(cc.URL, nil))
	defer proxySrv.Close()
	host, portStr := splitHostPort(proxySrv.URL[len("http://"):])

	p := pool.NewPool()
	pr := &model.Proxy{
		ID:       "cc1",
		Provider: "test",
		Kind:     model.KindIPPool,
		Scheme:   "http",
		Host:     host,
		Port:     atoi(portStr),
		Weight:   1,
		CheckURL: cc.URL,
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 2000, Concurrency: 8, MaxFails: 3}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should be alive")
	}
	if pr.Country != "HK" {
		t.Fatalf("country should be refreshed from check URL, got %q", pr.Country)
	}
}

func TestCheckerKeepsCountryWhenNoCountryDetected(t *testing.T) {
	// upstream returns only a bare IP (no country code)
	ip := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("103.156.242.197"))
	}))
	defer ip.Close()

	proxySrv := startProxyServer(fakeProxy(ip.URL, nil))
	defer proxySrv.Close()
	host, portStr := splitHostPort(proxySrv.URL[len("http://"):])

	p := pool.NewPool()
	pr := &model.Proxy{
		ID:       "cc2",
		Provider: "test",
		Kind:     model.KindIPPool,
		Scheme:   "http",
		Host:     host,
		Port:     atoi(portStr),
		Weight:   1,
		CheckURL: ip.URL,
		Country:  "US", // previously detected country
	}
	pr.Alive.Store(true)
	p.Add(pr)

	cfg := config.HealthConfig{IntervalSeconds: 1, TimeoutMs: 2000, Concurrency: 8, MaxFails: 3}
	c := NewChecker(cfg, p, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.checkAll(ctx)
	if !pr.Alive.Load() {
		t.Fatal("proxy should be alive")
	}
	if pr.Country != "US" {
		t.Fatalf("existing country should be preserved when check URL returns no country, got %q", pr.Country)
	}
}

func splitHostPort(addr string) (string, string) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:]
		}
	}
	return addr, ""
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}
