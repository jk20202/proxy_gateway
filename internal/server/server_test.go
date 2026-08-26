package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"proxy-pool/internal/alert"
	"proxy-pool/internal/auth"
	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
	"proxy-pool/internal/provider"
)

type mockProvider struct {
	kind model.Kind
}

func (m mockProvider) Name() string                                        { return "mock" }
func (m mockProvider) Kind() model.Kind                                    { return m.kind }
func (m mockProvider) Weight() int32                                       { return 1 }
func (m mockProvider) CheckURL() string                                    { return "" }
func (m mockProvider) Initial(ctx context.Context) ([]*model.Proxy, error) { return nil, nil }
func (m mockProvider) Refresh(ctx context.Context) ([]*model.Proxy, error) { return nil, nil }

type mockManager struct {
	provs  map[string]provider.Provider
	cfgMap map[string]config.ProviderCfg
}

func (m *mockManager) Providers() map[string]provider.Provider {
	return m.provs
}

func (m *mockManager) ProviderList() []pool.ProviderInfo {
	out := make([]pool.ProviderInfo, 0, len(m.cfgMap))
	for name, cfg := range m.cfgMap {
		out = append(out, pool.ProviderInfo{Name: name, Enabled: true, Weight: 1, Config: cfg})
	}
	return out
}
func (m *mockManager) ProviderConfig(name string) (config.ProviderCfg, bool) {
	cfg, ok := m.cfgMap[name]
	return cfg, ok
}
func (m *mockManager) SetProviderEnabled(name string, enabled bool) error {
	return nil
}
func (m *mockManager) SetProviderWeight(name string, weight int32) error { return nil }
func (m *mockManager) SetProviderPriority(name string, priority int, minRatio float64) error {
	return nil
}
func (m *mockManager) AddProvider(cfg config.ProviderCfg) error                 { return nil }
func (m *mockManager) RemoveProvider(name string) error                         { return nil }
func (m *mockManager) UpdateProvider(name string, cfg config.ProviderCfg) error { return nil }
func (m *mockManager) RefreshProvider(ctx context.Context, name string) error {
	return nil
}

func newTestServer(proxies []*model.Proxy) (*Server, *pool.Pool) {
	p := pool.NewPool()
	for _, pr := range proxies {
		p.Add(pr)
	}
	mgr := &mockManager{provs: map[string]provider.Provider{"mock": mockProvider{kind: model.KindTunnel}}}
	s := NewWithPool(p, mgr, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return s, p
}

func newTestDispatcher() *alert.Dispatcher {
	return alert.NewDispatcher(config.AlertConfig{DedupSeconds: 60}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func doRequest(h fasthttp.RequestHandler, method, path string, body []byte) *fasthttp.Response {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI(path)
	if body != nil {
		ctx.Request.SetBody(body)
	}
	h(ctx)
	return &ctx.Response
}

func newTunnelProxy(id string) *model.Proxy {
	pr := &model.Proxy{ID: id, Provider: "mock", Kind: model.KindTunnel, Scheme: "http", Host: "gw", Port: 8080, Weight: 1}
	pr.Alive.Store(true)
	return pr
}

func TestGetProxy(t *testing.T) {
	s, _ := newTestServer([]*model.Proxy{newTunnelProxy("t1")})
	h := s.Handler()

	resp := doRequest(h, "GET", "/api/v1/proxy", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
	var out struct {
		OK    bool          `json:"ok"`
		Proxy proxyResponse `json:"proxy"`
	}
	if err := json.Unmarshal(resp.Body(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK || out.Proxy.ID != "t1" || out.Proxy.Addr != "gw:8080" {
		t.Fatalf("bad response: %+v", out)
	}
}

func TestGetProxyEmpty(t *testing.T) {
	s, _ := newTestServer(nil)
	h := s.Handler()
	resp := doRequest(h, "GET", "/api/v1/proxy", nil)
	if resp.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status=%d", resp.StatusCode())
	}
}

func TestGetProxiesCount(t *testing.T) {
	proxies := make([]*model.Proxy, 0, 5)
	for i := range 5 {
		pr := &model.Proxy{ID: fmt.Sprintf("p%d", i), Provider: "mock", Kind: model.KindIPPool, Scheme: "http", Host: "h", Port: 8000 + i, Weight: 1}
		pr.Alive.Store(true)
		proxies = append(proxies, pr)
	}
	s, _ := newTestServer(proxies)
	h := s.Handler()

	resp := doRequest(h, "GET", "/api/v1/proxies?count=5", nil)
	var out struct {
		OK      bool            `json:"ok"`
		Proxies []proxyResponse `json:"proxies"`
	}
	if err := json.Unmarshal(resp.Body(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Proxies) != 5 {
		t.Fatalf("expected 5 proxies, got %d", len(out.Proxies))
	}
}

func TestReportSuccessAndFail(t *testing.T) {
	pr := newTunnelProxy("t1")
	s, _ := newTestServer([]*model.Proxy{pr})
	h := s.Handler()

	body, _ := json.Marshal(map[string]any{"success": false, "latency_ms": 50})
	resp := doRequest(h, "POST", "/api/v1/proxy/t1/report", body)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("report status=%d", resp.StatusCode())
	}

	for range 2 {
		doRequest(h, "POST", "/api/v1/proxy/t1/report", body)
	}
	if pr.Alive.Load() {
		t.Fatal("proxy should be dead after 3 fails")
	}

	body2, _ := json.Marshal(map[string]any{"success": true})
	doRequest(h, "POST", "/api/v1/proxy/t1/report", body2)
	if !pr.Alive.Load() {
		t.Fatal("proxy should recover after success")
	}
}

func TestHealthz(t *testing.T) {
	s, _ := newTestServer(nil)
	h := s.Handler()
	resp := doRequest(h, "GET", "/healthz", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
}

func TestNotFound(t *testing.T) {
	s, _ := newTestServer(nil)
	h := s.Handler()
	resp := doRequest(h, "GET", "/nope", nil)
	if resp.StatusCode() != fasthttp.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode())
	}
}

func TestHighConcurrencyLatency(t *testing.T) {
	proxies := make([]*model.Proxy, 0, 100)
	for i := range 100 {
		proxies = append(proxies, newTunnelProxy(fmt.Sprintf("p%d", i)))
	}
	s, _ := newTestServer(proxies)
	h := s.Handler()

	const workers = 64
	const perWorker = 20000
	var success int64
	start := time.Now()

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				resp := doRequest(h, "GET", "/api/v1/proxy", nil)
				if resp.StatusCode() == fasthttp.StatusOK {
					atomic.AddInt64(&success, 1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	reqs := int64(workers) * perWorker
	rps := float64(reqs) / elapsed.Seconds()

	t.Logf("handler: %d reqs in %v (%.0f rps), avg %.2f us/req, success=%d",
		reqs, elapsed, rps, float64(elapsed.Microseconds())/float64(reqs), success)

	if success != reqs {
		t.Fatalf("expected %d success, got %d", reqs, success)
	}
	if elapsed > 30*time.Second {
		t.Fatal("too slow")
	}
}

func TestProxyResponseJSONShape(t *testing.T) {
	s, _ := newTestServer([]*model.Proxy{newTunnelProxy("t1")})
	h := s.Handler()

	resp := doRequest(h, "GET", "/api/v1/proxy", nil)
	var out map[string]any
	if err := json.Unmarshal(resp.Body(), &out); err != nil {
		t.Fatal(err)
	}
	proxy := out["proxy"].(map[string]any)
	for _, key := range []string{"id", "scheme", "host", "port", "provider", "type", "addr"} {
		if _, ok := proxy[key]; !ok {
			t.Fatalf("missing key %q", key)
		}
	}
}

func TestAlertsConfigAPI(t *testing.T) {
	dis := newTestDispatcher()
	s, _ := newTestServer(nil)
	s.AttachAlerts(dis)
	h := s.Handler()

	// initial state
	resp := doRequest(h, "GET", "/api/v1/admin/alerts", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("get status=%d", resp.StatusCode())
	}
	if !bytes.Contains(resp.Body(), []byte(`"dedup_seconds":60`)) {
		t.Fatalf("unexpected config body: %s", resp.Body())
	}

	// add webhook
	body, _ := json.Marshal(config.WebhookConfig{URL: "https://example.com/hook", Events: []string{"provider_down"}})
	resp = doRequest(h, "POST", "/api/v1/admin/alerts/webhooks", body)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("add status=%d body=%s", resp.StatusCode(), resp.Body())
	}
	resp = doRequest(h, "GET", "/api/v1/admin/alerts", nil)
	if !bytes.Contains(resp.Body(), []byte("example.com/hook")) {
		t.Fatalf("webhook not visible: %s", resp.Body())
	}

	// duplicate rejected
	resp = doRequest(h, "POST", "/api/v1/admin/alerts/webhooks", body)
	if resp.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("duplicate add should be 400, got %d", resp.StatusCode())
	}

	// remove
	resp = doRequest(h, "DELETE", "/api/v1/admin/alerts/webhooks?url=https%3A%2F%2Fexample.com%2Fhook", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("remove status=%d body=%s", resp.StatusCode(), resp.Body())
	}
	resp = doRequest(h, "DELETE", "/api/v1/admin/alerts/webhooks?url=https%3A%2F%2Fexample.com%2Fhook", nil)
	if resp.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("remove missing should be 400, got %d", resp.StatusCode())
	}

	// email update
	ebody, _ := json.Marshal(config.EmailConfig{SMTPHost: "smtp.x.com", SMTPPort: 587, From: "a@x.com", To: []string{"b@x.com"}})
	resp = doRequest(h, "POST", "/api/v1/admin/alerts/email", ebody)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("email update status=%d body=%s", resp.StatusCode(), resp.Body())
	}

	// dedup update
	dbody, _ := json.Marshal(map[string]int{"dedup_seconds": 30})
	resp = doRequest(h, "POST", "/api/v1/admin/alerts/dedup", dbody)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("dedup update status=%d body=%s", resp.StatusCode(), resp.Body())
	}
	resp = doRequest(h, "GET", "/api/v1/admin/alerts", nil)
	if !bytes.Contains(resp.Body(), []byte(`"dedup_seconds":30`)) {
		t.Fatalf("dedup not updated: %s", resp.Body())
	}
	if !bytes.Contains(resp.Body(), []byte(`"smtp_host":"smtp.x.com"`)) {
		t.Fatalf("email not updated: %s", resp.Body())
	}
}

func TestAlertsConfigAPINotAttached(t *testing.T) {
	s, _ := newTestServer(nil)
	h := s.Handler()
	resp := doRequest(h, "GET", "/api/v1/admin/alerts", nil)
	if resp.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status=%d", resp.StatusCode())
	}
}

func TestAlertsRecoverAPI(t *testing.T) {
	s, _ := newTestServer(nil)
	dis := newTestDispatcher()
	s.AttachAlerts(dis)
	h := s.Handler()
	resp := doRequest(h, "POST", "/api/v1/admin/alerts/recover", []byte(`{"recover_interval_s":1800}`))
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("recover api failed: %d %s", resp.StatusCode(), resp.Body())
	}
	if cfg := dis.GetConfig(); cfg.RecoverSeconds != 1800 {
		t.Fatalf("recover not applied: %d", cfg.RecoverSeconds)
	}
}

func TestAuthDisabledStillWorks(t *testing.T) {
	s, _ := newTestServer([]*model.Proxy{newTunnelProxy("t1")})
	h := s.Handler()
	resp := doRequest(h, "GET", "/api/v1/proxy", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("auth-disabled proxy call failed: %d", resp.StatusCode())
	}
	resp = doRequest(h, "GET", "/api/v1/admin/providers", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("auth-disabled admin call failed: %d", resp.StatusCode())
	}
}

func TestAuthEnabledRejectsAnonymous(t *testing.T) {
	s, _ := newTestServer([]*model.Proxy{newTunnelProxy("t1")})
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "admin", Password: "pw", Token: "tok", Role: "admin", Enabled: true},
	}))
	h := s.Handler()

	resp := doRequest(h, "GET", "/api/v1/proxy", nil)
	if resp.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 for anonymous proxy, got %d", resp.StatusCode())
	}
	resp = doRequest(h, "GET", "/api/v1/admin/providers", nil)
	if resp.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 for anonymous admin, got %d", resp.StatusCode())
	}
}

func TestAuthTokenAllowed(t *testing.T) {
	s, _ := newTestServer([]*model.Proxy{newTunnelProxy("t1")})
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "admin", Password: "pw", Token: "tok", Role: "admin", Enabled: true},
	}))
	h := s.Handler()

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/proxy")
	ctx.Request.Header.Set("Authorization", "Bearer tok")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 with token, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
}

func TestAuthNonAdminCannotAdmin(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "user", Password: "pw", Token: "utok", Role: "user", Enabled: true},
	}))
	h := s.Handler()

	// accounts stay admin-only: non-admin is rejected
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/admin/accounts")
	ctx.Request.Header.Set("Authorization", "Bearer utok")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 for non-admin on accounts, got %d", ctx.Response.StatusCode())
	}

	// provider list is now open to any authenticated account
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/admin/providers")
	ctx.Request.Header.Set("Authorization", "Bearer utok")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 for non-admin on providers list, got %d", ctx.Response.StatusCode())
	}
}

func TestProviderOwnershipIsolation(t *testing.T) {
	s, _ := newTestServer(nil)
	s.mgr.(*mockManager).cfgMap = map[string]config.ProviderCfg{
		"mine":   {Name: "mine", Owner: "alice"},
		"theirs": {Name: "theirs", Owner: "bob"},
		"global": {Name: "global", Owner: ""},
	}
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "alice", Password: "pw", Token: "at", Role: "user", Enabled: true},
		{Name: "admin", Password: "pw", Token: "adm", Role: "admin", Enabled: true},
	}))
	h := s.Handler()

	req := func(method, path string) *fasthttp.Response {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(method)
		ctx.Request.SetRequestURI(path)
		ctx.Request.Header.Set("Authorization", "Bearer at")
		h(ctx)
		return &ctx.Response
	}

	// alice may delete her own provider (mock RemoveProvider is a no-op -> 200)
	resp := req("DELETE", "/api/v1/admin/providers/mine")
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 deleting own provider, got %d %s", resp.StatusCode(), resp.Body())
	}

	// alice may NOT delete someone else's private provider -> 403
	resp = req("DELETE", "/api/v1/admin/providers/theirs")
	if resp.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 deleting other's provider, got %d", resp.StatusCode())
	}

	// alice may NOT delete a global provider -> 403 (global owned by admin)
	resp = req("DELETE", "/api/v1/admin/providers/global")
	if resp.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 deleting global provider, got %d", resp.StatusCode())
	}

	// alice list sees her own private provider but not bob's
	body := req("GET", "/api/v1/admin/providers").Body()
	if !bytes.Contains(body, []byte(`"mine"`)) {
		t.Fatalf("expected to see own provider 'mine' in list, got %s", body)
	}
	if bytes.Contains(body, []byte(`"theirs"`)) {
		t.Fatalf("alice must not see bob's private provider, got %s", body)
	}
}

func TestGroupOwnershipIsolation(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachGroups([]config.GroupCfg{
		{Name: "glob", MinAliveRatio: 0, Primary: []string{"mock"}},
		{Name: "alicegrp", MinAliveRatio: 0, Primary: []string{"mock"}, Owner: "alice"},
		{Name: "bobgrp", MinAliveRatio: 0, Primary: []string{"mock"}, Owner: "bob"},
	})
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "alice", Password: "pw", Token: "at", Role: "user", Enabled: true},
	}))
	h := s.Handler()
	req := func(method, path string) *fasthttp.Response {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(method)
		ctx.Request.SetRequestURI(path)
		ctx.Request.Header.Set("Authorization", "Bearer at")
		h(ctx)
		return &ctx.Response
	}

	// alice CANNOT delete bob's private group -> 403
	resp := req("DELETE", "/api/v1/admin/groups?name=bobgrp")
	if resp.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 deleting bob's group, got %d", resp.StatusCode())
	}

	// alice CANNOT delete the global group -> 403 (admin-only)
	resp = req("DELETE", "/api/v1/admin/groups?name=glob")
	if resp.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 deleting global group, got %d", resp.StatusCode())
	}

	// alice CAN use her own private group via the proxy API
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/proxy?group=alicegrp")
	ctx.Request.Header.Set("Authorization", "Bearer at")
	h(ctx)
	if ctx.Response.StatusCode() == fasthttp.StatusForbidden {
		t.Fatalf("alice should be allowed her own group, got 403")
	}

	// alice CANNOT use bob's private group -> 403
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/proxy?group=bobgrp")
	ctx.Request.Header.Set("Authorization", "Bearer at")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 using bob's group, got %d", ctx.Response.StatusCode())
	}
}

func TestLoginEndpoint(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "admin", Password: "pw", Token: "tok", Role: "admin", Enabled: true},
	}))
	h := s.Handler()

	resp := doRequest(h, "POST", "/api/v1/auth/login", []byte(`{"name":"admin","password":"pw"}`))
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("login failed: %d %s", resp.StatusCode(), resp.Body())
	}
	if !bytes.Contains(resp.Body(), []byte(`"token":"tok"`)) {
		t.Fatalf("login did not return token: %s", resp.Body())
	}

	resp = doRequest(h, "POST", "/api/v1/auth/login", []byte(`{"name":"admin","password":"bad"}`))
	if resp.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 on bad password, got %d", resp.StatusCode())
	}
}

func TestAuthGroupPermission(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "alice", Password: "pw", Token: "at", Role: "user", Enabled: true, Groups: []string{"g1"}},
	}))
	s.AttachGroups([]config.GroupCfg{{Name: "g1", MinAliveRatio: 0, Primary: []string{"mock"}}, {Name: "g2", MinAliveRatio: 0, Primary: []string{"mock"}}})
	h := s.Handler()

	// g1 allowed
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/proxy?group=g1")
	ctx.Request.Header.Set("Authorization", "Bearer at")
	h(ctx)
	if ctx.Response.StatusCode() == fasthttp.StatusForbidden {
		t.Fatalf("g1 should be allowed for alice")
	}

	// g2 forbidden
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/api/v1/proxy?group=g2")
	ctx.Request.Header.Set("Authorization", "Bearer at")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected 403 for disallowed group, got %d", ctx.Response.StatusCode())
	}
}

func TestAdminAccountsAPI(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachAuth(auth.New([]config.AccountCfg{
		{Name: "admin", Password: "pw", Token: "atok", Role: "admin", Enabled: true},
	}))
	h := s.Handler()

	adminReq := func(method, path string, body []byte) *fasthttp.Response {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(method)
		ctx.Request.SetRequestURI(path)
		ctx.Request.Header.Set("Authorization", "Bearer atok")
		if body != nil {
			ctx.Request.SetBody(body)
		}
		h(ctx)
		return &ctx.Response
	}

	// add account
	resp := adminReq("POST", "/api/v1/admin/accounts", []byte(`{"name":"carol","password":"cp","token":"ctok","role":"user","enabled":true}`))
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("add account failed: %d %s", resp.StatusCode(), resp.Body())
	}

	// login as new account
	resp = doRequest(h, "POST", "/api/v1/auth/login", []byte(`{"name":"carol","password":"cp"}`))
	if resp.StatusCode() != fasthttp.StatusOK || !bytes.Contains(resp.Body(), []byte(`"token":"ctok"`)) {
		t.Fatalf("login as new account failed: %d %s", resp.StatusCode(), resp.Body())
	}

	// list accounts
	resp = adminReq("GET", "/api/v1/admin/accounts", nil)
	if resp.StatusCode() != fasthttp.StatusOK || !bytes.Contains(resp.Body(), []byte(`"carol"`)) {
		t.Fatalf("list accounts failed: %d %s", resp.StatusCode(), resp.Body())
	}
	if bytes.Contains(resp.Body(), []byte(`"cp"`)) {
		t.Fatalf("account list must not expose passwords: %s", resp.Body())
	}

	// remove account
	resp = adminReq("DELETE", "/api/v1/admin/accounts?name=carol", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("remove account failed: %d %s", resp.StatusCode(), resp.Body())
	}
	resp = doRequest(h, "POST", "/api/v1/auth/login", []byte(`{"name":"carol","password":"cp"}`))
	if resp.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 after removal, got %d", resp.StatusCode())
	}
}

func TestAdminGroupsAPI(t *testing.T) {
	s, _ := newTestServer(nil)
	s.AttachGroups([]config.GroupCfg{{Name: "g1", MinAliveRatio: 0.5, Primary: []string{"mock"}}})
	h := s.Handler()
	resp := doRequest(h, "GET", "/api/v1/admin/groups", nil)
	if resp.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("groups api failed: %d", resp.StatusCode())
	}
	if !bytes.Contains(resp.Body(), []byte(`"g1"`)) {
		t.Fatalf("groups api missing group: %s", resp.Body())
	}
}
