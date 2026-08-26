package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/geo"
	"proxy-pool/internal/model"
	"proxy-pool/internal/persist"
	"proxy-pool/internal/provider"
)

type managedProvider struct {
	provider provider.Provider
	cfg      config.ProviderCfg
	enabled  bool
	weight   int32
	lastErr  string
	lastOK   time.Time
}

type Manager struct {
	mu      sync.RWMutex
	pool    *Pool
	provs   map[string]*managedProvider
	cfg     *config.Config
	logger  *slog.Logger
	persist *persist.MySQL
	redis   *persist.Redis

	geo *geo.Client // nil 时不做 IP 归属查询（geo 未启用）

	// AlertEmit is an optional hook invoked on provider events (refresh
	// failures, etc.). It avoids a hard dependency on the alert package.
	AlertEmit func(eventType, provider, message string, data map[string]any)
}

func (m *Manager) emit(eventType, provider, message string, data map[string]any) {
	if m.AlertEmit != nil {
		m.AlertEmit(eventType, provider, message, data)
	}
}

func NewManager(cfg *config.Config, logger *slog.Logger) (*Manager, error) {
	m := &Manager{
		pool:   NewPool(),
		provs:  make(map[string]*managedProvider),
		cfg:    cfg,
		logger: logger,
	}
	for _, pc := range cfg.Providers {
		if _, exists := m.provs[pc.Name]; exists {
			return nil, fmt.Errorf("duplicate provider name %q", pc.Name)
		}
		p, err := provider.New(pc)
		if err != nil {
			return nil, err
		}
		m.provs[pc.Name] = &managedProvider{
			provider: p,
			cfg:      pc,
			enabled:  pc.Enabled,
			weight:   int32(pc.Weight),
		}
	}
	return m, nil
}

func (m *Manager) Pool() *Pool {
	return m.pool
}

// SetGeo wires the IP geolocation client used to enrich proxy countries.
func (m *Manager) SetGeo(g *geo.Client) {
	m.geo = g
}

// AttachMySQL wires optional MySQL persistence for provider configs so runtime
// edits survive restarts.
func (m *Manager) AttachMySQL(p *persist.MySQL) {
	m.mu.Lock()
	m.persist = p
	m.mu.Unlock()
}

// SyncProvidersToDB seeds MySQL with the current provider configs. Called once
// at startup so config.yaml providers appear even when the table was empty.
func (m *Manager) SyncProvidersToDB() error {
	m.mu.RLock()
	ps := make([]config.ProviderCfg, 0, len(m.provs))
	for _, mp := range m.provs {
		ps = append(ps, mp.cfg)
	}
	pers := m.persist
	m.mu.RUnlock()
	if pers == nil {
		return nil
	}
	return pers.ReplaceProviders(ps)
}

// AttachRedis wires optional Redis caching of proxy runtime state.
func (m *Manager) AttachRedis(r *persist.Redis) {
	m.mu.Lock()
	m.redis = r
	m.mu.Unlock()
}

// RestoreFromRedis replays cached latency / country values onto the proxies in
// the pool. Called once after the initial load so restart preserves the last
// observed health data; the health checker re-verifies alive soon after.
func (m *Manager) RestoreFromRedis() {
	m.mu.RLock()
	r := m.redis
	m.mu.RUnlock()
	if r == nil {
		return
	}
	byProvider := map[string][]*model.Proxy{}
	for _, pr := range m.pool.All() {
		byProvider[pr.Provider] = append(byProvider[pr.Provider], pr)
	}
	for prov, proxies := range byProvider {
		states, err := r.LoadProxyStates(prov)
		if err != nil {
			m.logger.Debug("redis restore failed", "provider", prov, "err", err)
			continue
		}
		restored := 0
		for _, pr := range proxies {
			st, ok := states[pr.ID]
			if !ok {
				continue
			}
			if st.LatencyMS > 0 {
				pr.LatencyMS.Store(st.LatencyMS)
			}
			if st.Country != "" {
				pr.Country = st.Country
			}
			restored++
		}
		if restored > 0 {
			m.logger.Info("restored proxy state from redis", "provider", prov, "proxies", restored)
		}
	}
	m.pool.Rebuild()
}

// addProxies pushes proxies into the pool. When the provider config carries an
// explicit country it is stamped directly; otherwise enrichment is deferred to
// the geo loop.
func (m *Manager) addProxies(cfg config.ProviderCfg, proxies []*model.Proxy) {
	for _, pr := range proxies {
		if cfg.Country != "" {
			pr.Country = cfg.Country
		}
		m.pool.Add(pr)
	}
}

// EnrichCountries periodically resolves the country of proxies whose country
// is still unknown. It batches IPs per provider and only queries public IPs
// (private / loopback addresses are skipped by the geo client). A provider
// whose config sets a fixed country never reaches this loop.
func (m *Manager) EnrichCountries(ctx context.Context) {
	if m.geo == nil {
		return
	}
	interval := time.Duration(m.cfg.Geo.IntervalSecs) * time.Second
	if interval <= 0 {
		interval = 60 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.enrichOnce(ctx)
		}
	}
}

func (m *Manager) enrichOnce(ctx context.Context) {
	unresolved := m.pool.UnresolvedCountries()
	if len(unresolved) == 0 {
		return
	}
	ips := make([]string, 0, len(unresolved))
	for ip := range unresolved {
		ips = append(ips, ip)
	}
	resolved := m.geo.Lookup(ctx, ips)
	updated := 0
	for ip, ids := range unresolved {
		country := resolved[ip]
		if country == "" {
			continue
		}
		for _, id := range ids {
			m.pool.SetCountry(id, country)
			updated++
		}
	}
	if updated > 0 {
		m.logger.Info("geo enrichment applied", "proxies", updated, "batch", len(ips))
	}
}

func (m *Manager) Providers() map[string]provider.Provider {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]provider.Provider, len(m.provs))
	for name, mp := range m.provs {
		out[name] = mp.provider
	}
	return out
}

type ProviderInfo struct {
	Name          string             `json:"name"`
	Type          string             `json:"type"`
	ProviderType  string             `json:"provider_type"` // cfg.Type：tunnel / ip_pool / sticky / free
	Enabled       bool               `json:"enabled"`
	Weight        int32              `json:"weight"`
	Priority      int                `json:"priority"`
	CheckInterval int                `json:"check_interval_s"`
	MinAliveRatio float64            `json:"min_alive_ratio"`
	StickySeconds int                `json:"sticky_seconds"`
	Total         int                `json:"total"`
	Alive         int                `json:"alive"`
	Countries     map[string]int     `json:"countries"` // country code -> 代理数
	LastErr       string             `json:"last_err"`
	LastOK        time.Time          `json:"last_ok"`
	Config        config.ProviderCfg `json:"config,omitempty"`
}

func (m *Manager) ProviderList() []ProviderInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := m.pool.StatsByProvider()
	aliveByProv := map[string]int{}
	totalByProv := map[string]int{}
	for _, st := range stats {
		aliveByProv[st.Provider] = st.Alive
		totalByProv[st.Provider] = st.Total
	}
	out := make([]ProviderInfo, 0, len(m.provs))
	for name, mp := range m.provs {
		out = append(out, ProviderInfo{
			Name:          name,
			Type:          mp.provider.Kind().String(),
			ProviderType:  mp.cfg.Type,
			Enabled:       mp.enabled,
			Weight:        mp.weight,
			Priority:      mp.cfg.Priority,
			CheckInterval: mp.cfg.CheckIntervalS,
			MinAliveRatio: mp.cfg.MinAliveRatio,
			StickySeconds: mp.cfg.StickySeconds,
			Total:         totalByProv[name],
			Alive:         aliveByProv[name],
			Countries:     m.countryDistribution(name),
			LastErr:       mp.lastErr,
			LastOK:        mp.lastOK,
			Config:        mp.cfg,
		})
	}
	return out
}

// countryDistribution counts proxies per country code for one provider.
func (m *Manager) countryDistribution(provider string) map[string]int {
	out := map[string]int{}
	for _, pr := range m.pool.All() {
		if pr.Provider != provider {
			continue
		}
		if pr.Country == "" {
			out["unknown"]++
		} else {
			out[pr.Country]++
		}
	}
	return out
}

func (m *Manager) GetProvider(name string) (provider.Provider, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mp, ok := m.provs[name]
	if !ok {
		return nil, false
	}
	return mp.provider, true
}

// ProviderConfig returns the immutable config of a provider, used for
// ownership checks before mutating or deleting it.
func (m *Manager) ProviderConfig(name string) (config.ProviderCfg, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mp, ok := m.provs[name]
	if !ok {
		return config.ProviderCfg{}, false
	}
	return mp.cfg, true
}

func (m *Manager) AddProvider(cfg config.ProviderCfg) error {
	if cfg.Name == "" {
		return fmt.Errorf("provider name is required")
	}
	p, err := provider.New(cfg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if _, exists := m.provs[cfg.Name]; exists {
		m.mu.Unlock()
		return fmt.Errorf("provider %q already exists", cfg.Name)
	}
	mp := &managedProvider{
		provider: p,
		cfg:      cfg,
		enabled:  cfg.Enabled,
		weight:   int32(cfg.Weight),
	}
	if mp.weight <= 0 {
		mp.weight = 1
	}
	m.provs[cfg.Name] = mp
	m.mu.Unlock()

	if cfg.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		proxies, err := p.Initial(ctx)
		if err != nil {
			m.logger.Warn("new provider initial load failed", "provider", cfg.Name, "err", err)
		} else {
			m.addProxies(cfg, proxies)
			m.logger.Info("provider added", "provider", cfg.Name, "proxies", len(proxies))
		}
	}
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider", "provider", cfg.Name, "err", err)
		}
	}
	return nil
}

func (m *Manager) RemoveProvider(name string) error {
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	delete(m.provs, name)
	m.mu.Unlock()

	for _, pr := range m.pool.All() {
		if pr.Provider == name {
			m.pool.Remove(pr.ID)
		}
	}
	if m.persist != nil {
		if err := m.persist.DeleteProvider(name); err != nil {
			m.logger.Error("failed to delete provider from mysql", "provider", name, "err", err)
		}
	}
	m.logger.Info("provider removed", "provider", name, "type", mp.provider.Kind().String())
	return nil
}

// UpdateProvider replaces the runtime configuration of an existing provider.
// The provider name is preserved; all other fields are taken from cfg. When
// the provider is enabled, its existing proxies are dropped and re-fetched
// with the new configuration.
func (m *Manager) UpdateProvider(name string, cfg config.ProviderCfg) error {
	if name == "" {
		return fmt.Errorf("provider name is required")
	}
	cfg.Name = name
	p, err := provider.New(cfg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	wasEnabled := mp.enabled
	mp.provider = p
	mp.cfg = cfg
	mp.weight = int32(cfg.Weight)
	if mp.weight <= 0 {
		mp.weight = 1
	}
	m.mu.Unlock()

	for _, pr := range m.pool.All() {
		if pr.Provider == name {
			m.pool.Remove(pr.ID)
		}
	}
	if wasEnabled {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		proxies, err := p.Initial(ctx)
		if err != nil {
			m.logger.Warn("provider update initial load failed", "provider", name, "err", err)
			return err
		}
		m.addProxies(cfg, proxies)
		m.logger.Info("provider updated", "provider", name, "proxies", len(proxies))
	} else {
		m.logger.Info("provider updated (disabled)", "provider", name)
	}
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider update", "provider", name, "err", err)
		}
	}
	return nil
}

func (m *Manager) SetProviderEnabled(name string, enabled bool) error {
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	if mp.enabled == enabled {
		m.mu.Unlock()
		return nil
	}
	mp.enabled = enabled
	mp.cfg.Enabled = enabled
	cfg := mp.cfg
	m.mu.Unlock()

	if enabled {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		proxies, err := mp.provider.Initial(ctx)
		if err != nil {
			m.logger.Warn("provider enable failed", "provider", name, "err", err)
			return err
		}
		m.addProxies(mp.cfg, proxies)
		m.logger.Info("provider enabled", "provider", name, "proxies", len(proxies))
	} else {
		for _, pr := range m.pool.All() {
			if pr.Provider == name {
				m.pool.Remove(pr.ID)
			}
		}
		m.logger.Info("provider disabled", "provider", name)
	}
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider enabled", "provider", name, "err", err)
		}
	}
	return nil
}

func (m *Manager) SetProviderPublic(name string, public bool) error {
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	if mp.cfg.Public == public {
		m.mu.Unlock()
		return nil
	}
	mp.cfg.Public = public
	cfg := mp.cfg
	m.mu.Unlock()
	m.logger.Info("provider public updated", "provider", name, "public", public)
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider public", "provider", name, "err", err)
		}
	}
	return nil
}

func (m *Manager) SetProviderWeight(name string, weight int32) error {
	if weight <= 0 {
		weight = 1
	}
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	mp.weight = weight
	mp.cfg.Weight = int(weight)
	cfg := mp.cfg
	m.mu.Unlock()

	for _, pr := range m.pool.All() {
		if pr.Provider == name {
			pr.Weight = weight
		}
	}
	m.pool.Rebuild()
	m.logger.Info("provider weight updated", "provider", name, "weight", weight)
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider weight", "provider", name, "err", err)
		}
	}
	return nil
}

func (m *Manager) SetProviderPriority(name string, priority int, minRatio float64) error {
	if minRatio < 0 || minRatio > 1 {
		return fmt.Errorf("min_alive_ratio must be in [0,1]")
	}
	m.mu.Lock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("provider %q not found", name)
	}
	mp.cfg.Priority = priority
	mp.cfg.MinAliveRatio = minRatio
	cfg := mp.cfg
	m.mu.Unlock()

	for _, pr := range m.pool.All() {
		if pr.Provider == name {
			pr.Priority = priority
			pr.MinAliveRatio = minRatio
		}
	}
	m.pool.Rebuild()
	m.logger.Info("provider priority updated", "provider", name, "priority", priority, "min_alive_ratio", minRatio)
	if m.persist != nil {
		if err := m.persist.SaveProvider(cfg); err != nil {
			m.logger.Error("failed to persist provider priority", "provider", name, "err", err)
		}
	}
	return nil
}

func (m *Manager) RefreshProvider(ctx context.Context, name string) error {
	m.mu.RLock()
	mp, ok := m.provs[name]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("provider %q not found", name)
	}
	m.mu.RUnlock()

	proxies, err := mp.provider.Refresh(ctx)
	if err != nil {
		m.mu.Lock()
		mp.lastErr = err.Error()
		m.mu.Unlock()
		m.logger.Warn("provider refresh failed", "provider", name, "err", err)
		m.emit("refresh_failed", name, "provider "+name+" refresh failed: "+err.Error(), map[string]any{
			"error": err.Error(),
		})
		return err
	}
	if proxies == nil {
		return nil
	}
	m.mu.RLock()
	cfg := mp.cfg
	m.mu.RUnlock()
	m.addProxies(cfg, proxies)
	removed := m.pool.RemoveExpired()
	m.mu.Lock()
	mp.lastErr = ""
	mp.lastOK = time.Now()
	m.mu.Unlock()
	m.logger.Info("provider refreshed", "provider", name, "proxies", len(proxies), "expired_removed", removed)
	return nil
}

func (m *Manager) LoadInitial(ctx context.Context) {
	var wg sync.WaitGroup
	m.mu.RLock()
	type entry struct {
		name string
		mp   *managedProvider
	}
	entries := make([]entry, 0, len(m.provs))
	for name, mp := range m.provs {
		if mp.enabled {
			entries = append(entries, entry{name, mp})
		}
	}
	m.mu.RUnlock()

	for _, e := range entries {
		wg.Add(1)
		go func(name string, mp *managedProvider) {
			defer wg.Done()
			proxies, err := mp.provider.Initial(ctx)
			if err != nil {
				m.logger.Warn("provider initial load failed", "provider", name, "err", err)
				m.mu.Lock()
				mp.lastErr = err.Error()
				m.mu.Unlock()
				return
			}
			m.addProxies(mp.cfg, proxies)
			m.mu.Lock()
			mp.lastOK = time.Now()
			m.mu.Unlock()
			m.logger.Info("provider loaded", "provider", name, "proxies", len(proxies))
		}(e.name, e.mp)
	}
	wg.Wait()
}

func (m *Manager) RefreshLoop(ctx context.Context) {
	for {
		m.mu.RLock()
		type entry struct {
			name     string
			mp       *managedProvider
			interval time.Duration
		}
		entries := make([]entry, 0)
		for name, mp := range m.provs {
			if !mp.enabled {
				continue
			}
			interval := providerRefreshInterval(&mp.cfg)
			if interval > 0 {
				entries = append(entries, entry{name, mp, interval})
			}
		}
		m.mu.RUnlock()

		for _, e := range entries {
			e := e
			go func(e entry) {
				ticker := time.NewTicker(e.interval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						_ = m.RefreshProvider(ctx, e.name)
					}
				}
			}(e)
		}
		return
	}
}

func providerRefreshInterval(cfg *config.ProviderCfg) time.Duration {
	if cfg.IPPool != nil && cfg.IPPool.RefreshSeconds > 0 {
		return time.Duration(cfg.IPPool.RefreshSeconds) * time.Second
	}
	if cfg.Free != nil && cfg.Free.RefreshSeconds > 0 {
		return time.Duration(cfg.Free.RefreshSeconds) * time.Second
	}
	return 0
}

var _ = model.KindTunnel
