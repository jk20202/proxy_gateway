package server

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"proxy-pool/internal/auth"
	"proxy-pool/internal/config"
	"proxy-pool/internal/gateway"
	"proxy-pool/internal/health"
	"proxy-pool/internal/model"
	"proxy-pool/internal/persist"
	"proxy-pool/internal/pool"
	"proxy-pool/internal/store"
)

//go:embed web/index.html web/login.html
var webFS embed.FS

type ProviderManager interface {
	ProviderList() []pool.ProviderInfo
	ProviderConfig(name string) (config.ProviderCfg, bool)
	SetProviderEnabled(name string, enabled bool) error
	SetProviderWeight(name string, weight int32) error
	SetProviderPriority(name string, priority int, minRatio float64) error
	AddProvider(cfg config.ProviderCfg) error
	RemoveProvider(name string) error
	UpdateProvider(name string, cfg config.ProviderCfg) error
	RefreshProvider(ctx context.Context, name string) error
}

// AlertsManager is implemented by the alert dispatcher and allows the admin
// API to inspect and mutate the runtime alert configuration.
type AlertsManager interface {
	GetConfig() config.AlertConfig
	AddWebhook(wh config.WebhookConfig) error
	UpdateWebhook(oldURL string, wh config.WebhookConfig) error
	RemoveWebhook(url string) error
	UpdateEmail(e config.EmailConfig) error
	UpdateDedup(seconds int) error
	UpdateMonitorSeconds(seconds int) error
	UpdateRecoverSeconds(seconds int) error
}

type Server struct {
	cfg          config.ServerConfig
	pool         *pool.Pool
	logger       *slog.Logger
	mgr          ProviderManager
	checker      *health.Checker
	emitter      func(eventType, provider, message string, data map[string]any)
	alerts       AlertsManager
	auth         *auth.Manager
	usage        *store.Store
	groups       []config.GroupCfg
	groupsFile   string
	groupsMu     sync.RWMutex
	groupsFromDB bool
	gateway      *gateway.Gateway
	db           *persist.MySQL
}

func New(cfg config.Config, mgr *pool.Manager, checker *health.Checker, logger *slog.Logger) *Server {
	return &Server{
		cfg:     cfg.Server,
		pool:    mgr.Pool(),
		logger:  logger,
		mgr:     mgr,
		checker: checker,
		emitter: mgr.AlertEmit,
		groups:  cfg.Groups,
	}
}

func NewWithPool(p *pool.Pool, mgr ProviderManager, checker *health.Checker, logger *slog.Logger) *Server {
	return &Server{
		pool:    p,
		logger:  logger,
		mgr:     mgr,
		checker: checker,
	}
}

// AttachAlerts wires a runtime alert config manager (optional).
func (s *Server) AttachAlerts(a AlertsManager) {
	s.alerts = a
}

// AttachAuth wires the account manager (optional; nil disables auth).
func (s *Server) AttachAuth(a *auth.Manager) {
	s.auth = a
}

// AttachUsage wires the async usage recorder (optional).
func (s *Server) AttachUsage(u *store.Store) {
	s.usage = u
}

// AttachGroups wires explicit group scheduling config (optional).
func (s *Server) AttachGroups(g []config.GroupCfg) {
	s.groups = g
}

// AttachMySQL wires optional MySQL persistence for group configs. When groups
// already exist in the database they take precedence over the file/config so
// runtime edits survive restarts.
func (s *Server) AttachMySQL(db *persist.MySQL) {
	s.db = db
	if db == nil {
		return
	}
	fromDB, err := db.LoadGroups()
	if err != nil {
		s.logger.Error("failed to load groups from mysql", "err", err)
		return
	}
	if len(fromDB) > 0 {
		s.groupsMu.Lock()
		s.groups = fromDB
		s.groupsFromDB = true
		s.groupsMu.Unlock()
		s.pool.SetGroups(fromDB)
	} else if len(s.groups) > 0 {
		// seed the database from the configured groups on first start
		if err := db.ReplaceGroups(s.groups); err != nil {
			s.logger.Error("failed to seed groups into mysql", "err", err)
		}
	}
}

type handlerFunc func(ctx *fasthttp.RequestCtx)

type route struct {
	method string
	path   string
	handle handlerFunc
}

func (s *Server) Handler() fasthttp.RequestHandler {
	routes := []route{
		{"GET", "/api/v1/proxy", s.handleGetProxy},
		{"GET", "/api/v1/proxies", s.handleGetProxies},
		{"GET", "/api/v1/status", s.handleStatus},
		{"POST", "/api/v1/feedback", s.handleFeedback},
		{"GET", "/healthz", s.handleHealthz},
		{"POST", "/api/v1/auth/login", s.handleLogin},
		{"GET", "/api/v1/auth/me", s.handleMe},

		{"GET", "/api/v1/admin/providers", s.handleListProviders},
		{"POST", "/api/v1/admin/providers", s.handleAddProvider},
		{"DELETE", "/api/v1/admin/providers", s.handleRemoveProvider},
		{"PUT", "/api/v1/admin/providers/{name}", s.handleUpdateProvider},
		{"GET", "/api/v1/admin/proxies", s.handleListProxies},
		{"POST", "/api/v1/admin/health/check", s.handleHealthCheck},
		{"GET", "/api/v1/admin/groups", s.handleListGroups},
		{"POST", "/api/v1/admin/groups", s.handleAddGroup},
		{"DELETE", "/api/v1/admin/groups", s.handleRemoveGroup},
		{"PUT", "/api/v1/admin/groups/{name}", s.handleUpdateGroup},
		{"GET", "/api/v1/admin/accounts", s.handleListAccounts},
		{"POST", "/api/v1/admin/accounts", s.handleAddAccount},
		{"PUT", "/api/v1/admin/accounts/{name}", s.handleUpdateAccount},
		{"DELETE", "/api/v1/admin/accounts", s.handleRemoveAccount},
		{"GET", "/api/v1/admin/usage", s.handleUsage},

		{"GET", "/api/v1/admin/alerts", s.handleGetAlerts},
		{"POST", "/api/v1/admin/alerts/webhooks", s.handleAddWebhook},
		{"PUT", "/api/v1/admin/alerts/webhooks", s.handleUpdateWebhook},
		{"DELETE", "/api/v1/admin/alerts/webhooks", s.handleRemoveWebhook},
		{"POST", "/api/v1/admin/alerts/email", s.handleUpdateEmail},
		{"POST", "/api/v1/admin/alerts/dedup", s.handleUpdateDedup},
		{"POST", "/api/v1/admin/alerts/monitor", s.handleUpdateMonitor},
		{"POST", "/api/v1/admin/alerts/recover", s.handleUpdateRecover},
	}

	indexHTML, _ := webFS.ReadFile("web/index.html")
	indexHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetContentType("text/html; charset=utf-8")
		ctx.SetBody(indexHTML)
	}

	loginHTML, _ := webFS.ReadFile("web/login.html")
	loginHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetContentType("text/html; charset=utf-8")
		ctx.SetBody(loginHTML)
	}

	notFound := func(ctx *fasthttp.RequestCtx) {
		ctx.Error("not found", fasthttp.StatusNotFound)
	}

	return func(ctx *fasthttp.RequestCtx) {
		method := string(ctx.Method())
		path := string(ctx.Path())

		if path == "/" || path == "/index.html" {
			if method == "GET" {
				indexHandler(ctx)
				return
			}
			ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
			return
		}

		if path == "/login" {
			if method == "GET" {
				loginHandler(ctx)
				return
			}
			ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
			return
		}

		if method == "POST" && path == "/api/v1/auth/login" {
			s.handleLogin(ctx)
			return
		}

		if method == "POST" && strings.HasPrefix(path, "/api/v1/proxy/") && strings.HasSuffix(path, "/report") {
			if !s.authorize(ctx, false) {
				return
			}
			s.handleReport(ctx)
			return
		}

		// Path-based gateway endpoint: authenticates with group credentials
		// (Basic) and forwards to the target URL via a live proxy. This keeps
		// the gateway usable behind preview reverse proxies that only forward
		// plain path requests.
		if path == "/api/v1/gw" {
			s.handleGatewayProxy(ctx)
			return
		}

		if strings.HasPrefix(path, "/api/v1/admin/") {
			// Provider / group / proxy management is available to any
			// authenticated account; ownership checks inside each handler
			// restrict what a non-admin may read or mutate. Everything else
			// under /admin (accounts, usage, alerts, health) stays admin-only.
			adminOnly := true
			switch {
			case strings.HasPrefix(path, "/api/v1/admin/providers"):
				adminOnly = false
			case strings.HasPrefix(path, "/api/v1/admin/groups"):
				adminOnly = false
			case strings.HasPrefix(path, "/api/v1/admin/proxies"):
				adminOnly = false
			}
			if !s.authorize(ctx, adminOnly) {
				return
			}
		} else if strings.HasPrefix(path, "/api/v1/") {
			if !s.authorize(ctx, false) {
				return
			}
		}

		if strings.HasPrefix(path, "/api/v1/admin/providers/") {
			if s.handleProviderAction(ctx, method, path) {
				return
			}
			if method == "PUT" {
				s.handleUpdateProvider(ctx)
				return
			}
			if method == "DELETE" {
				s.handleRemoveProviderByName(ctx)
				return
			}
			notFound(ctx)
			return
		}

		if strings.HasPrefix(path, "/api/v1/admin/groups/") && method == "PUT" {
			s.handleUpdateGroup(ctx)
			return
		}

		if strings.HasPrefix(path, "/api/v1/admin/accounts/") && method == "PUT" {
			s.handleUpdateAccount(ctx)
			return
		}

		if method == "DELETE" && strings.HasPrefix(path, "/api/v1/admin/proxies/") {
			s.handleRemoveProxy(ctx)
			return
		}

		for _, r := range routes {
			if r.method == method && r.path == path {
				r.handle(ctx)
				return
			}
		}

		notFound(ctx)
	}
}

// authorize validates the request against the account manager. When no
// accounts are configured, auth is disabled and all requests pass. Admin
// routes additionally require an admin role.
func (s *Server) authorize(ctx *fasthttp.RequestCtx, admin bool) bool {
	if s.auth == nil || s.auth.Empty() {
		ctx.SetUserValue("account", "")
		return true
	}
	token := bearerToken(ctx)
	if token == "" {
		writeJSON(ctx, fasthttp.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return false
	}
	acct, ok := s.auth.ByToken(token)
	if !ok {
		writeJSON(ctx, fasthttp.StatusUnauthorized, map[string]string{"error": "invalid token"})
		return false
	}
	if admin && !acct.IsAdmin() {
		writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "admin role required"})
		return false
	}
	ctx.SetUserValue("account", acct.Name)
	ctx.SetUserValue("accountObj", acct)
	return true
}

func bearerToken(ctx *fasthttp.RequestCtx) string {
	h := string(ctx.Request.Header.Peek("Authorization"))
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	return strings.TrimSpace(string(ctx.Request.Header.Peek("X-Api-Token")))
}

func currentAccount(ctx *fasthttp.RequestCtx) string {
	v, _ := ctx.UserValue("account").(string)
	return v
}

func currentAccountObj(ctx *fasthttp.RequestCtx) *auth.Account {
	v, _ := ctx.UserValue("accountObj").(*auth.Account)
	return v
}

// isAdminUser reports whether the requesting account holds the admin role.
func (s *Server) isAdminUser(ctx *fasthttp.RequestCtx) bool {
	acct := currentAccountObj(ctx)
	return acct != nil && acct.IsAdmin()
}

// providerVisible reports whether the account may see a provider: admins and
// global providers are always visible; otherwise the provider must be owned by
// the account or marked public.
func (s *Server) providerVisible(ctx *fasthttp.RequestCtx, cfg config.ProviderCfg) bool {
	if s.isAdminUser(ctx) {
		return true
	}
	if cfg.Owner == "" {
		return true
	}
	acct := currentAccount(ctx)
	if acct == "" {
		return true
	}
	if cfg.Owner == acct {
		return true
	}
	return cfg.Public
}

// providerWritable reports whether the account may mutate a provider: admins
// may mutate anything, others only their own providers.
func (s *Server) providerWritable(ctx *fasthttp.RequestCtx, cfg config.ProviderCfg) bool {
	if s.isAdminUser(ctx) {
		return true
	}
	acct := currentAccount(ctx)
	if acct == "" {
		return true
	}
	return cfg.Owner == acct
}

// groupVisible reports whether the account may see/use a group: admins and
// global groups are always available; private groups belong to their owner.
func (s *Server) groupVisible(ctx *fasthttp.RequestCtx, g config.GroupCfg) bool {
	if s.isAdminUser(ctx) {
		return true
	}
	if g.Owner == "" {
		return true
	}
	acct := currentAccount(ctx)
	if acct == "" {
		return true
	}
	return g.Owner == acct
}

// groupWritable reports whether the account may mutate a group: admins may
// mutate any group, others only their own private groups. Global groups are
// managed exclusively by admins.
func (s *Server) groupWritable(ctx *fasthttp.RequestCtx, g config.GroupCfg) bool {
	if s.isAdminUser(ctx) {
		return true
	}
	acct := currentAccount(ctx)
	if acct == "" {
		return true
	}
	return g.Owner != "" && g.Owner == acct
}

// findGroup returns the group with the given name.
func (s *Server) findGroup(name string) (config.GroupCfg, bool) {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	for _, g := range s.groups {
		if g.Name == name {
			return g, true
		}
	}
	return config.GroupCfg{}, false
}

// requireOwnedGroup resolves a group by name and verifies the requesting
// account is allowed to mutate it. Returns (cfg, ok); on failure a 403/404
// response has already been written.
func (s *Server) requireOwnedGroup(ctx *fasthttp.RequestCtx, name string) (config.GroupCfg, bool) {
	g, found := s.findGroup(name)
	if !found {
		writeJSON(ctx, fasthttp.StatusNotFound, map[string]string{"error": "group not found: " + name})
		return config.GroupCfg{}, false
	}
	if !s.groupWritable(ctx, g) {
		writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "access denied: you do not own this group"})
		return config.GroupCfg{}, false
	}
	return g, true
}

// groupAllowed checks whether the requesting account may consume from the
// given group. When no group is requested, true is returned.
func (s *Server) groupAllowed(ctx *fasthttp.RequestCtx, group string) bool {
	if group == "" {
		return true
	}
	acct := currentAccountObj(ctx)
	if acct == nil {
		return true
	}
	g, found := s.findGroup(group)
	if !found {
		return false
	}
	if acct.IsAdmin() {
		return true
	}
	// Private groups are usable only by their owner; global groups are
	// subject to the account's group whitelist.
	if g.Owner != "" {
		return g.Owner == acct.Name
	}
	return acct.CanUseGroup(group)
}

// recordCall pushes a usage record asynchronously.
func (s *Server) recordCall(acct, group, provider, id, addr string, sticky bool) {
	if s.usage == nil || acct == "" {
		return
	}
	kind := ""
	if provider != "" {
		kind = "proxy"
	}
	s.usage.Record(store.Call{
		Account: acct,
		Group:   group,
		ProxyID: id,
		Addr:    addr,
		Kind:    kind,
		OK:      true,
	})
}

func writeJSON(ctx *fasthttp.RequestCtx, status int, v any) {
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(status)
	_ = json.NewEncoder(ctx).Encode(v)
}

type proxyResponse struct {
	ID       string `json:"id"`
	Scheme   string `json:"scheme"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Provider string `json:"provider"`
	Type     string `json:"type"`
	Addr     string `json:"addr"`
}

func (s *Server) handleGetProxy(ctx *fasthttp.RequestCtx) {
	sticky, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("sticky_seconds")))
	clientID := string(ctx.QueryArgs().Peek("client_id"))
	group := string(ctx.QueryArgs().Peek("group"))
	acct := currentAccount(ctx)
	if !s.groupAllowed(ctx, group) {
		writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "group not allowed for account"})
		return
	}
	if sticky > 0 && clientID != "" {
		pr := s.pool.StickyNext(clientID, sticky, group)
		if pr == nil {
			s.emitPoolExhausted()
			s.recordCall(acct, group, "", "", "", false)
			writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "no available proxy"})
			return
		}
		s.recordCall(acct, group, pr.Provider, pr.ID, pr.Addr(), pr.Kind == model.KindSticky)
		writeJSON(ctx, fasthttp.StatusOK, map[string]any{
			"ok":    true,
			"proxy": toResponse(pr),
			"sticky": map[string]any{
				"client_id":      clientID,
				"sticky_seconds": sticky,
			},
		})
		return
	}
	var pr *model.Proxy
	if group != "" {
		pr = s.pool.NextInGroup(group)
	} else {
		pr = s.pool.Next()
	}
	if pr == nil {
		s.emitPoolExhausted()
		s.recordCall(acct, group, "", "", "", false)
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "no available proxy"})
		return
	}
	s.recordCall(acct, group, pr.Provider, pr.ID, pr.Addr(), false)
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{
		"ok":    true,
		"proxy": toResponse(pr),
	})
}

func (s *Server) emitPoolExhausted() {
	if s.emitter != nil {
		s.emitter("pool_exhausted", "", "no available proxy for request", nil)
	}
}

func (s *Server) handleGetProxies(ctx *fasthttp.RequestCtx) {
	n, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("count")))
	if n <= 0 {
		n = 1
	}
	if n > 100 {
		n = 100
	}
	group := string(ctx.QueryArgs().Peek("group"))
	acct := currentAccount(ctx)
	if !s.groupAllowed(ctx, group) {
		writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "group not allowed for account"})
		return
	}

	exclude := make(map[string]struct{}, n)
	out := make([]proxyResponse, 0, n)
	for range n {
		var pr *model.Proxy
		if group != "" {
			pr = s.pool.NextInGroupExcluding(group, exclude)
		} else {
			pr = s.pool.NextExcluding(exclude)
		}
		if pr == nil {
			break
		}
		exclude[pr.ID] = struct{}{}
		out = append(out, toResponse(pr))
	}
	if len(out) == 0 {
		s.emitPoolExhausted()
		s.recordCall(acct, group, "", "", "", false)
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "no available proxy"})
		return
	}
	s.recordCall(acct, group, out[0].Provider, out[0].ID, out[0].Addr, false)
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{
		"ok":      true,
		"proxies": out,
	})
}

func toResponse(pr *model.Proxy) proxyResponse {
	return proxyResponse{
		ID:       pr.ID,
		Scheme:   pr.Scheme,
		Host:     pr.Host,
		Port:     pr.Port,
		Username: pr.Username,
		Password: pr.Password,
		Provider: pr.Provider,
		Type:     pr.Kind.String(),
		Addr:     pr.Addr(),
	}
}

type reportRequest struct {
	Success   bool `json:"success"`
	LatencyMS int  `json:"latency_ms"`
}

func (s *Server) handleReport(ctx *fasthttp.RequestCtx) {
	pathParts := strings.Split(string(ctx.Path()), "/")
	if len(pathParts) < 5 {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "proxy id required"})
		return
	}
	id := pathParts[len(pathParts)-2]
	req := reportRequest{}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if req.Success {
		s.pool.MarkSuccess(id, int64(req.LatencyMS))
	} else {
		s.pool.MarkFailed(id, int64(req.LatencyMS))
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleFeedback(ctx *fasthttp.RequestCtx) {
	var req struct {
		ID        string `json:"id"`
		Success   bool   `json:"success"`
		LatencyMS int    `json:"latency_ms"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if req.ID == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "proxy id required"})
		return
	}
	if req.Success {
		s.pool.MarkSuccess(req.ID, int64(req.LatencyMS))
	} else {
		s.pool.MarkFailed(req.ID, int64(req.LatencyMS))
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleHealthz(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	_, _ = ctx.WriteString("ok")
}

func (s *Server) handleStatus(ctx *fasthttp.RequestCtx) {
	total, alive := s.pool.Stats()
	byProvider := s.providerStats()
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{
		"ok":          true,
		"total":       total,
		"alive":       alive,
		"by_provider": byProvider,
		"groups":      s.pool.GroupStats(),
	})
}

func (s *Server) providerStats() map[string]any {
	type stat struct {
		Provider      string  `json:"provider"`
		Type          string  `json:"type"`
		Total         int     `json:"total"`
		Alive         int     `json:"alive"`
		Enabled       bool    `json:"enabled"`
		Weight        int32   `json:"weight"`
		Priority      int     `json:"priority"`
		MinAliveRatio float64 `json:"min_alive_ratio"`
		StickySeconds int     `json:"sticky_seconds"`
	}
	enabledByProv := map[string]bool{}
	weightByProv := map[string]int32{}
	kindByProv := map[string]string{}
	priorityByProv := map[string]int{}
	minRatioByProv := map[string]float64{}
	stickyByProv := map[string]int{}
	for _, pi := range s.mgr.ProviderList() {
		enabledByProv[pi.Name] = pi.Enabled
		weightByProv[pi.Name] = pi.Weight
		kindByProv[pi.Name] = pi.Type
		priorityByProv[pi.Name] = pi.Priority
		minRatioByProv[pi.Name] = pi.MinAliveRatio
		stickyByProv[pi.Name] = pi.StickySeconds
	}
	stats := make([]stat, 0, len(enabledByProv))
	seen := map[string]bool{}
	for _, ps := range s.pool.StatsByProvider() {
		stats = append(stats, stat{
			Provider:      ps.Provider,
			Type:          kindByProv[ps.Provider],
			Total:         ps.Total,
			Alive:         ps.Alive,
			Enabled:       enabledByProv[ps.Provider],
			Weight:        weightByProv[ps.Provider],
			Priority:      priorityByProv[ps.Provider],
			MinAliveRatio: minRatioByProv[ps.Provider],
			StickySeconds: stickyByProv[ps.Provider],
		})
		seen[ps.Provider] = true
	}
	for name, en := range enabledByProv {
		if !seen[name] {
			stats = append(stats, stat{
				Provider: name, Type: kindByProv[name], Enabled: en,
				Weight: weightByProv[name], Priority: priorityByProv[name],
				MinAliveRatio: minRatioByProv[name], StickySeconds: stickyByProv[name],
			})
		}
	}
	return map[string]any{"providers": stats}
}

// ---- Admin API ----

func (s *Server) handleListProviders(ctx *fasthttp.RequestCtx) {
	all := s.mgr.ProviderList()
	visible := all[:0]
	for _, p := range all {
		if s.providerVisible(ctx, p.Config) {
			visible = append(visible, p)
		}
	}
	resp := map[string]any{"providers": visible}
	if s.checker != nil {
		resp["default_check_urls"] = s.checker.DefaultCheckURLs()
	}
	writeJSON(ctx, fasthttp.StatusOK, resp)
}

// providerAccessDenied writes a uniform 403 response.
func (s *Server) providerAccessDenied(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "access denied: you do not own this provider"})
}

func (s *Server) handleAddProvider(ctx *fasthttp.RequestCtx) {
	var cfg config.ProviderCfg
	if err := json.Unmarshal(ctx.PostBody(), &cfg); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	// A non-admin can only create a provider owned by themselves. They may
	// choose to share it publicly, but the owner is always themselves. Admin
	// may create global providers (Owner="") or providers owned by any account.
	if !s.isAdminUser(ctx) {
		cfg.Owner = currentAccount(ctx)
	}
	if err := s.mgr.AddProvider(cfg); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// requireOwnedProvider resolves a provider by name and verifies the requesting
// account is allowed to mutate it. Returns (cfg, ok); on failure a 403/404
// response has already been written.
func (s *Server) requireOwnedProvider(ctx *fasthttp.RequestCtx, name string) (config.ProviderCfg, bool) {
	cfg, found := s.mgr.ProviderConfig(name)
	if !found {
		writeJSON(ctx, fasthttp.StatusNotFound, map[string]string{"error": "provider not found"})
		return config.ProviderCfg{}, false
	}
	if !s.providerWritable(ctx, cfg) {
		s.providerAccessDenied(ctx)
		return config.ProviderCfg{}, false
	}
	return cfg, true
}

func (s *Server) handleRemoveProvider(ctx *fasthttp.RequestCtx) {
	name := string(ctx.QueryArgs().Peek("name"))
	if name == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}
	if _, ok := s.requireOwnedProvider(ctx, name); !ok {
		return
	}
	if err := s.mgr.RemoveProvider(name); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// handleRemoveProviderByName deletes a provider referenced by its name in the
// URL path (DELETE /api/v1/admin/providers/{name}), matching how the web
// console calls the endpoint.
func (s *Server) handleRemoveProviderByName(ctx *fasthttp.RequestCtx) {
	rest := strings.TrimPrefix(string(ctx.Path()), "/api/v1/admin/providers/")
	rest = strings.TrimSuffix(rest, "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}
	name := parts[0]
	if _, ok := s.requireOwnedProvider(ctx, name); !ok {
		return
	}
	if err := s.mgr.RemoveProvider(name); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateProvider(ctx *fasthttp.RequestCtx) {
	rest := strings.TrimPrefix(string(ctx.Path()), "/api/v1/admin/providers/")
	rest = strings.TrimSuffix(rest, "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}
	name := parts[0]
	if _, ok := s.requireOwnedProvider(ctx, name); !ok {
		return
	}
	var cfg config.ProviderCfg
	if err := json.Unmarshal(ctx.PostBody(), &cfg); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	// Preserve ownership on update: a non-admin must keep their own provider
	// private to themselves, and may not transfer it or make it global. They
	// may, however, flip the public-share flag.
	if !s.isAdminUser(ctx) {
		cfg.Owner = currentAccount(ctx)
	}
	if err := s.mgr.UpdateProvider(name, cfg); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleProviderAction(ctx *fasthttp.RequestCtx, method, path string) bool {
	rest := strings.TrimPrefix(path, "/api/v1/admin/providers/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return false
	}
	name := parts[0]
	if name == "" {
		return false
	}
	// Every action (enable/disable/weight/refresh/priority) mutates the
	// provider, so enforce ownership before dispatching.
	if _, ok := s.requireOwnedProvider(ctx, name); !ok {
		return true
	}
	action := parts[1]

	switch action {
	case "enable":
		if method != "POST" {
			return false
		}
		if err := s.mgr.SetProviderEnabled(name, true); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
			return true
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
		return true
	case "disable":
		if method != "POST" {
			return false
		}
		if err := s.mgr.SetProviderEnabled(name, false); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
			return true
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
		return true
	case "weight":
		if method != "POST" {
			return false
		}
		var req struct {
			Weight int32 `json:"weight"`
		}
		if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
			return true
		}
		if err := s.mgr.SetProviderWeight(name, req.Weight); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
			return true
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
		return true
	case "refresh":
		if method != "POST" {
			return false
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.mgr.RefreshProvider(ctx2, name); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
			return true
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
		return true
	case "priority":
		if method != "POST" {
			return false
		}
		var req struct {
			Priority      int     `json:"priority"`
			MinAliveRatio float64 `json:"min_alive_ratio"`
		}
		if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
			return true
		}
		if err := s.mgr.SetProviderPriority(name, req.Priority, req.MinAliveRatio); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
			return true
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
		return true
	}
	return false
}

func (s *Server) handleListProxies(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"proxies": s.pool.ProxyList()})
}

func (s *Server) handleRemoveProxy(ctx *fasthttp.RequestCtx) {
	var id string
	if q := ctx.QueryArgs().Peek("id"); len(q) > 0 {
		id = string(q)
	} else {
		rest := strings.TrimPrefix(string(ctx.Path()), "/api/v1/admin/proxies/")
		if rest != "" && rest != string(ctx.Path()) {
			id = rest
		}
	}
	if id == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "proxy id required"})
		return
	}
	if !s.pool.Remove(id) {
		writeJSON(ctx, fasthttp.StatusNotFound, map[string]string{"error": "proxy not found"})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleHealthCheck(ctx *fasthttp.RequestCtx) {
	if s.checker == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "health checker not configured"})
		return
	}
	ctx2, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.checker.CheckOnce(ctx2)
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// ---- Alert Config Admin API ----

func (s *Server) alertsOrError(ctx *fasthttp.RequestCtx) bool {
	if s.alerts == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "alert dispatcher not configured"})
		return false
	}
	return true
}

func (s *Server) handleGetAlerts(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, s.alerts.GetConfig())
}

func (s *Server) handleAddWebhook(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var wh config.WebhookConfig
	if err := json.Unmarshal(ctx.PostBody(), &wh); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.alerts.AddWebhook(wh); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleRemoveWebhook(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	url := string(ctx.QueryArgs().Peek("url"))
	if err := s.alerts.RemoveWebhook(url); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateWebhook(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var req struct {
		OldURL string   `json:"old_url"`
		URL    string   `json:"url"`
		Secret string   `json:"secret"`
		Events []string `json:"events"`
		Remark string   `json:"remark"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if req.OldURL == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "old_url required"})
		return
	}
	wh := config.WebhookConfig{URL: req.URL, Secret: req.Secret, Events: req.Events, Remark: req.Remark}
	if err := s.alerts.UpdateWebhook(req.OldURL, wh); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateEmail(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var e config.EmailConfig
	if err := json.Unmarshal(ctx.PostBody(), &e); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.alerts.UpdateEmail(e); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateDedup(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var req struct {
		DedupSeconds int `json:"dedup_seconds"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.alerts.UpdateDedup(req.DedupSeconds); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateMonitor(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var req struct {
		MonitorSeconds int `json:"monitor_interval_s"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.alerts.UpdateMonitorSeconds(req.MonitorSeconds); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateRecover(ctx *fasthttp.RequestCtx) {
	if !s.alertsOrError(ctx) {
		return
	}
	var req struct {
		RecoverSeconds int `json:"recover_interval_s"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.alerts.UpdateRecoverSeconds(req.RecoverSeconds); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// ---- Auth & Account Admin API ----

func (s *Server) handleLogin(ctx *fasthttp.RequestCtx) {
	if s.auth == nil || s.auth.Empty() {
		writeJSON(ctx, fasthttp.StatusForbidden, map[string]string{"error": "auth not configured"})
		return
	}
	var req struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	acct, ok := s.auth.Login(req.Name, req.Password)
	if !ok {
		writeJSON(ctx, fasthttp.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{
		"ok":     true,
		"token":  acct.Token,
		"name":   acct.Name,
		"role":   acct.Role,
		"groups": acct.Groups,
		"admin":  acct.IsAdmin(),
	})
}

// handleMe returns the current authenticated account (used by the web console
// to decide which endpoints to show). Works for non-admin tokens too, so the
// console can hide admin-only endpoints from regular users. When auth is not
// configured, all endpoints are open and the effective role is admin.
func (s *Server) handleMe(ctx *fasthttp.RequestCtx) {
	if s.auth == nil || s.auth.Empty() {
		writeJSON(ctx, fasthttp.StatusOK, map[string]any{
			"name": "", "role": "admin", "groups": []string{}, "admin": true,
		})
		return
	}
	acct := currentAccountObj(ctx)
	if acct == nil {
		writeJSON(ctx, fasthttp.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{
		"name":   acct.Name,
		"role":   acct.Role,
		"groups": acct.Groups,
		"admin":  acct.IsAdmin(),
	})
}

func (s *Server) handleListAccounts(ctx *fasthttp.RequestCtx) {
	if s.auth == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "auth not configured"})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"accounts": s.auth.List()})
}

func (s *Server) handleAddAccount(ctx *fasthttp.RequestCtx) {
	if s.auth == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "auth not configured"})
		return
	}
	var req config.AccountCfg
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	// New accounts are enabled by default unless the caller explicitly sets
	// enabled=false, so a minimal POST body still yields an active account.
	var raw map[string]any
	if json.Unmarshal(ctx.PostBody(), &raw) == nil {
		if v, ok := raw["enabled"].(bool); ok {
			req.Enabled = v
		} else {
			req.Enabled = true
		}
	}
	if err := s.auth.AddAccount(req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleRemoveAccount(ctx *fasthttp.RequestCtx) {
	if s.auth == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "auth not configured"})
		return
	}
	name := string(ctx.QueryArgs().Peek("name"))
	if err := s.auth.RemoveAccount(name); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleUpdateAccount(ctx *fasthttp.RequestCtx) {
	if s.auth == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "auth not configured"})
		return
	}
	name := strings.TrimPrefix(string(ctx.Path()), "/api/v1/admin/accounts/")
	name, _, _ = strings.Cut(name, "/")
	if name == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "account name required"})
		return
	}
	var req config.AccountCfg
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if err := s.auth.UpdateAccount(name, req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// handleGatewayProxy serves the path-based gateway endpoint. It authenticates
// with group credentials via Basic auth (Proxy-Authorization or
// Authorization header) and forwards a GET request to the target URL through a
// live proxy from the matched group.
func (s *Server) handleGatewayProxy(ctx *fasthttp.RequestCtx) {
	if s.gateway == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "gateway not configured"})
		return
	}
	user, pass, ok := basicCredentials(ctx)
	if !ok {
		ctx.Response.Header.Set("Proxy-Authenticate", `Basic realm="proxy-pool"`)
		ctx.SetStatusCode(fasthttp.StatusProxyAuthRequired)
		ctx.SetBodyString("proxy authentication required\n")
		return
	}
	group := s.gateway.LookupGroup(user, pass)
	if group == "" {
		s.logger.Warn("gateway path proxy auth failed", "user", user)
		ctx.Response.Header.Set("Proxy-Authenticate", `Basic realm="proxy-pool"`)
		ctx.SetStatusCode(fasthttp.StatusProxyAuthRequired)
		ctx.SetBodyString("proxy authentication required\n")
		return
	}
	target := string(ctx.QueryArgs().Peek("url"))
	if target == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "url query parameter required"})
		return
	}
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid url"})
		return
	}
	status, hdr, body, err := s.gateway.ForwardPlain(u, group)
	if err != nil {
		s.logger.Warn("gateway path proxy forward failed", "group", group, "err", err)
		writeJSON(ctx, fasthttp.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	for k, vv := range hdr {
		for _, v := range vv {
			if strings.EqualFold(k, "Content-Length") {
				continue
			}
			ctx.Response.Header.Add(k, v)
		}
	}
	ctx.SetStatusCode(status)
	ctx.SetBody(body)
}

// basicCredentials extracts Basic credentials from Proxy-Authorization (proxy
// clients) or Authorization (plain curl -u) headers.
func basicCredentials(ctx *fasthttp.RequestCtx) (user, pass string, ok bool) {
	h := string(ctx.Request.Header.Peek("Proxy-Authorization"))
	if h == "" {
		h = string(ctx.Request.Header.Peek("Authorization"))
	}
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

func (s *Server) handleListGroups(ctx *fasthttp.RequestCtx) {
	stats := s.pool.GroupStats()
	cfgMap := map[string]config.GroupCfg{}
	s.groupsMu.RLock()
	for _, g := range s.groups {
		if s.groupVisible(ctx, g) {
			cfgMap[g.Name] = g
		}
	}
	s.groupsMu.RUnlock()

	if len(stats) == 0 && len(cfgMap) > 0 {
		// groups configured but pool snapshot not yet built
		out := make([]map[string]any, 0, len(cfgMap))
		for _, g := range s.groups {
			if !s.groupVisible(ctx, g) {
				continue
			}
			tiers := []map[string]any{{"name": "primary", "alive_total": 0, "alive_count": 0, "min_alive_ratio": g.MinAliveRatio, "usable": false}}
			for _, b := range g.Backups {
				tiers = append(tiers, map[string]any{"name": b.Name, "alive_total": 0, "alive_count": 0, "min_alive_ratio": b.MinAliveRatio, "usable": false})
			}
			out = append(out, map[string]any{
				"group": g.Name, "type": g.Type, "tiers": tiers,
				"primary": g.Primary, "primary_weights": g.PrimaryWeights, "backups": g.Backups,
				"min_alive_ratio": g.MinAliveRatio, "username": g.Username, "password": g.Password,
				"regions": g.Regions, "owner": g.Owner,
			})
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]any{"groups": out})
		return
	}
	out := make([]map[string]any, 0, len(stats))
	for _, g := range stats {
		cfg, ok := cfgMap[g.Group]
		if !ok {
			// pool has a group the requesting account cannot see
			continue
		}
		_ = ok
		m := map[string]any{
			"group": g.Group, "type": g.Type, "tiers": g.Tiers,
			"primary": cfg.Primary, "primary_weights": cfg.PrimaryWeights, "backups": cfg.Backups,
			"min_alive_ratio": cfg.MinAliveRatio,
			"username":        cfg.Username,
			"password":        cfg.Password,
			"regions":         cfg.Regions,
			"owner":           cfg.Owner,
		}
		out = append(out, m)
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"groups": out})
}

// handleAddGroup creates a new scheduling group and hot-swaps the pool.
func (s *Server) handleAddGroup(ctx *fasthttp.RequestCtx) {
	var g config.GroupCfg
	if err := json.Unmarshal(ctx.PostBody(), &g); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	// Non-admins always create groups owned by themselves. Only admins can
	// create global groups (Owner="") shared by every account.
	if !s.isAdminUser(ctx) {
		g.Owner = currentAccount(ctx)
	}
	if err := s.validateGroup(g); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.groupsMu.Lock()
	for _, existing := range s.groups {
		if existing.Name == g.Name {
			s.groupsMu.Unlock()
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "group already exists: " + g.Name})
			return
		}
	}
	s.groups = append(s.groups, g)
	s.groupsMu.Unlock()
	s.applyGroups()
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// handleUpdateGroup updates an existing group by name.
func (s *Server) handleUpdateGroup(ctx *fasthttp.RequestCtx) {
	name := groupNameFromPath(ctx)
	if name == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}
	if _, ok := s.requireOwnedGroup(ctx, name); !ok {
		return
	}
	var g config.GroupCfg
	if err := json.Unmarshal(ctx.PostBody(), &g); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	g.Name = name // path name is authoritative (rename unsupported)
	// Preserve ownership on update: non-admins may not turn their group into
	// a global one, and may not claim a global group.
	if !s.isAdminUser(ctx) {
		g.Owner = currentAccount(ctx)
	}
	if err := s.validateGroup(g); err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.groupsMu.Lock()
	found := false
	for i := range s.groups {
		if s.groups[i].Name == name {
			s.groups[i] = g
			found = true
			break
		}
	}
	s.groupsMu.Unlock()
	if !found {
		writeJSON(ctx, fasthttp.StatusNotFound, map[string]string{"error": "group not found: " + name})
		return
	}
	s.applyGroups()
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

// handleRemoveGroup deletes a group by name (query param).
func (s *Server) handleRemoveGroup(ctx *fasthttp.RequestCtx) {
	name := string(ctx.QueryArgs().Peek("name"))
	if name == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}
	if _, ok := s.requireOwnedGroup(ctx, name); !ok {
		return
	}
	s.groupsMu.Lock()
	found := false
	for i := range s.groups {
		if s.groups[i].Name == name {
			s.groups = append(s.groups[:i], s.groups[i+1:]...)
			found = true
			break
		}
	}
	s.groupsMu.Unlock()
	if !found {
		writeJSON(ctx, fasthttp.StatusNotFound, map[string]string{"error": "group not found: " + name})
		return
	}
	if s.db != nil {
		if err := s.db.DeleteGroup(name); err != nil {
			s.logger.Error("failed to delete group from mysql", "name", name, "err", err)
			writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": "failed to delete group from mysql"})
			return
		}
	}
	s.applyGroups()
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"ok": true})
}

func groupNameFromPath(ctx *fasthttp.RequestCtx) string {
	rest := strings.TrimPrefix(string(ctx.Path()), "/api/v1/admin/groups/")
	rest = strings.TrimSuffix(rest, "/")
	return rest
}

// validateGroup checks that referenced providers exist and ratio is in range.
func (s *Server) validateGroup(g config.GroupCfg) error {
	if g.Name == "" {
		return errors.New("group name is required")
	}
	if g.MinAliveRatio < 0 || g.MinAliveRatio > 1 {
		return errors.New("min_alive_ratio must be in [0,1]")
	}
	if (g.Username == "") != (g.Password == "") {
		return errors.New("gateway username and password must be configured together (or both empty)")
	}
	for _, r := range g.Regions {
		if !config.ValidRegion(r) {
			return errors.New("invalid region " + r + ": want domestic, overseas or an ISO alpha-2 country code (e.g. US/HK)")
		}
	}
	if len(g.Primary) == 0 {
		return errors.New("at least one primary provider is required")
	}
	known := map[string]bool{}
	for _, p := range s.mgr.ProviderList() {
		known[p.Name] = true
	}
	for _, pn := range g.Primary {
		if !known[pn] {
			return errors.New("primary references unknown provider: " + pn)
		}
	}
	for pn, w := range g.PrimaryWeights {
		if w < 1 {
			return errors.New("primary_weights[" + pn + "] must be >= 1")
		}
		inPrimary := false
		for _, prim := range g.Primary {
			if prim == pn {
				inPrimary = true
				break
			}
		}
		if !inPrimary {
			return errors.New("primary_weights references " + pn + " which is not in primary")
		}
	}
	// primary_weights are usage shares expressed as percentages; when set for
	// every primary provider they must sum to 100 so the pool balances to the
	// intended load split.
	if len(g.PrimaryWeights) == len(g.Primary) {
		sum := 0
		for _, w := range g.PrimaryWeights {
			sum += w
		}
		if sum != 100 {
			return errors.New("primary_weights must sum to 100 when specified for all primary providers")
		}
	}
	for bi := range g.Backups {
		b := &g.Backups[bi]
		if b.Name == "" {
			return errors.New("backup pool name is required")
		}
		if b.MinAliveRatio < 0 || b.MinAliveRatio > 1 {
			return errors.New("backup min_alive_ratio must be in [0,1]")
		}
		for _, pn := range b.Providers {
			if !known[pn] {
				return errors.New("backup references unknown provider: " + pn)
			}
		}
	}
	return nil
}

// applyGroups pushes the current group list to the pool and gateway and
// persists to the JSON file if configured.
func (s *Server) applyGroups() {
	s.groupsMu.RLock()
	snapshot := make([]config.GroupCfg, len(s.groups))
	copy(snapshot, s.groups)
	s.groupsMu.RUnlock()
	s.pool.SetGroups(snapshot)
	if s.gateway != nil {
		s.gateway.Rebuild()
	}
	if s.db != nil {
		if err := s.db.ReplaceGroups(snapshot); err != nil {
			s.logger.Error("failed to persist groups to mysql", "err", err)
		}
	}
	if s.groupsFile != "" {
		_ = s.persistGroups(snapshot)
	}
}

// GroupList returns a snapshot of the current group definitions.
func (s *Server) GroupList() []config.GroupCfg {
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	out := make([]config.GroupCfg, len(s.groups))
	copy(out, s.groups)
	return out
}

// SetGroupsFile enables JSON persistence and loads existing file if present.
// When groups were already loaded from MySQL they take precedence, so the file
// is only used as a fallback source when no database is configured.
func (s *Server) SetGroupsFile(path string) error {
	s.groupsFile = path
	s.groupsMu.RLock()
	fromDB := s.groupsFromDB
	s.groupsMu.RUnlock()
	if fromDB {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var groups []config.GroupCfg
	if err := json.Unmarshal(data, &groups); err != nil {
		return err
	}
	if len(groups) > 0 {
		s.groupsMu.Lock()
		s.groups = groups
		s.groupsMu.Unlock()
		s.pool.SetGroups(groups)
	}
	return nil
}

func (s *Server) persistGroups(groups []config.GroupCfg) error {
	data, err := json.MarshalIndent(groups, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.groupsFile, data, 0o600)
}

// AttachGateway wires the proxy gateway so group CRUD refreshes its creds.
func (s *Server) AttachGateway(g *gateway.Gateway) {
	s.gateway = g
}

func (s *Server) handleUsage(ctx *fasthttp.RequestCtx) {
	if s.usage == nil {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"error": "usage recording not configured"})
		return
	}
	account := string(ctx.QueryArgs().Peek("account"))
	from, _ := strconv.ParseInt(string(ctx.QueryArgs().Peek("from")), 10, 64)
	to, _ := strconv.ParseInt(string(ctx.QueryArgs().Peek("to")), 10, 64)
	if account != "" {
		n, err := s.usage.Count(account, from, to)
		if err != nil {
			writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(ctx, fasthttp.StatusOK, map[string]any{"account": account, "total": n})
		return
	}
	summary, err := s.usage.Summary(from, to)
	if err != nil {
		writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"accounts": summary})
}
