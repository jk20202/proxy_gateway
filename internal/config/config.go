package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig  `yaml:"server" json:"server"`
	HealthCheck HealthConfig  `yaml:"health_check" json:"health_check"`
	Providers   []ProviderCfg `yaml:"providers" json:"providers"`
	Groups      []GroupCfg    `yaml:"groups" json:"groups"`
	Accounts    []AccountCfg  `yaml:"accounts" json:"accounts"`
	DB          DBConfig      `yaml:"db" json:"db"`
	Alerts      AlertConfig   `yaml:"alerts" json:"alerts"`
	Geo         GeoConfig     `yaml:"geo" json:"geo"`
	Storage     StorageConfig `yaml:"storage" json:"storage"`
}

// StorageConfig configures persistent storage: MySQL for low-frequency settings
// (accounts / groups / provider configs) and Redis for high-frequency proxy
// runtime state (latency / country / alive). Both are optional; when disabled
// the process falls back to in-memory state (accounts/groups persisted to JSON
// files as before).
type StorageConfig struct {
	MySQL MySQLConfig `yaml:"mysql" json:"mysql"`
	Redis RedisConfig `yaml:"redis" json:"redis"`
}

type MySQLConfig struct {
	DSN  string `yaml:"dsn" json:"dsn"` // e.g. proxy:proxy_pass@tcp(127.0.0.1:3306)/proxy_pool
	Addr string `yaml:"addr" json:"addr"`
	User string `yaml:"user" json:"user"`
	Pass string `yaml:"pass" json:"pass"`
	DB   string `yaml:"db" json:"db"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	Password string `yaml:"password" json:"password"`
	DB       int    `yaml:"db" json:"db"`
}

// GeoConfig configures IP geolocation used to tag each proxy with its country.
// When enabled, proxies are enriched asynchronously after entering the pool.
type GeoConfig struct {
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	URL          string `yaml:"url" json:"url"`                             // batch endpoint (default http://ip-api.com/batch)
	IntervalSecs int    `yaml:"enrich_interval_s" json:"enrich_interval_s"` // 每次回填扫描间隔；0=默认 60s
}

// GroupCfg defines a named scheduling group: a primary pool plus an ordered
// list of fallback pools. When the primary pool's alive ratio drops below
// MinAliveRatio, the first fallback pool is used; if it also fails, the next
// fallback is tried, and so on. Once a pool is usable, lower fallbacks are
// never considered.
type GroupCfg struct {
	Name          string   `yaml:"name" json:"name"`
	Type          string   `yaml:"type" json:"type"` // e.g. tunnel / static / high-quality
	MinAliveRatio float64  `yaml:"min_alive_ratio" json:"min_alive_ratio"`
	Primary       []string `yaml:"primary" json:"primary"` // provider names
	// PrimaryWeights 主池中各 provider 的调度权重（provider 名 -> 权重）。
	// 未指定的 provider 权重为 1。权重已从 Provider 层迁移到分组主池层配置。
	PrimaryWeights map[string]int `yaml:"primary_weights" json:"primary_weights"`
	Backups        []BackupPool   `yaml:"backups" json:"backups"`
	// Regions restricts which proxies may enter this group, matched against the
	// proxy's country code. Values are "domestic" (CN 中国内地), "overseas"
	// (一切非中国内地，含中国香港 HK) or a specific ISO alpha-2 code (e.g.
	// "US", "JP", "HK"). Empty = 不限. 命中任一条件即允许入池。
	Regions []string `yaml:"regions" json:"regions"`
	// Gateway credentials: when set, this group can be consumed through the
	// proxy gateway using `http://username:password@host:port`. The gateway
	// authenticates against these and forwards traffic via a live proxy from
	// this group (auto failover across tiers is transparent to the client).
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	// Owner 归属账户名：空=全局分组（所有用户可用，通常由 admin 创建）；非空=
	// 私有分组，仅 owner 本人与 admin 可查看/编辑/删除/使用。私有分组的网关
	// 凭证同样只对 owner 开放。
	Owner string `yaml:"owner" json:"owner"`
}

// BackupPool is one fallback tier of a group. It may reference multiple
// providers (tunnel, static or ip_pool types are all allowed).
type BackupPool struct {
	Name          string   `yaml:"name" json:"name"`
	MinAliveRatio float64  `yaml:"min_alive_ratio" json:"min_alive_ratio"`
	Providers     []string `yaml:"providers" json:"providers"`
}

// AccountCfg is an internal user. Password is used for web login, Token for
// API consumption calls. Groups restricts which scheduling groups the account
// may use (empty = all groups).
type AccountCfg struct {
	Name     string   `yaml:"name" json:"name"`
	Password string   `yaml:"password" json:"password"`
	Token    string   `yaml:"token" json:"token"`
	Role     string   `yaml:"role" json:"role"` // admin / user
	Enabled  bool     `yaml:"enabled" json:"enabled"`
	Groups   []string `yaml:"groups" json:"groups"`
}

// DBConfig configures the optional SQLite usage store.
type DBConfig struct {
	Path            string `yaml:"path" json:"path"`
	BatchSize       int    `yaml:"batch_size" json:"batch_size"`
	FlushIntervalMs int    `yaml:"flush_interval_ms" json:"flush_interval_ms"`
}

type AlertConfig struct {
	Webhooks       []WebhookConfig `yaml:"webhooks" json:"webhooks"`
	Email          EmailConfig     `yaml:"email" json:"email"`
	DedupSeconds   int             `yaml:"dedup_seconds" json:"dedup_seconds"`
	MonitorSeconds int             `yaml:"monitor_interval_s" json:"monitor_interval_s"`
	RecoverSeconds int             `yaml:"recover_interval_s" json:"recover_interval_s"`
}

type WebhookConfig struct {
	URL    string   `yaml:"url" json:"url"`
	Secret string   `yaml:"secret" json:"secret"` // optional: sent as X-Webhook-Secret header for signature validation
	Events []string `yaml:"events" json:"events"`
	Remark string   `yaml:"remark" json:"remark"` // optional: human-readable note to identify the webhook
}

type EmailConfig struct {
	SMTPHost string   `yaml:"smtp_host" json:"smtp_host"`
	SMTPPort int      `yaml:"smtp_port" json:"smtp_port"`
	Username string   `yaml:"username" json:"username"`
	Password string   `yaml:"password" json:"password"`
	From     string   `yaml:"from" json:"from"`
	To       []string `yaml:"to" json:"to"`
	UseTLS   bool     `yaml:"use_tls" json:"use_tls"`
	Events   []string `yaml:"events" json:"events"`
}

type ServerConfig struct {
	Listen       string `yaml:"listen"`
	ReadTimeout  int    `yaml:"read_timeout_ms"`
	WriteTimeout int    `yaml:"write_timeout_ms"`
	MaxWorkers   int    `yaml:"max_workers"`
	// GatewayListen is the optional address for the HTTP proxy gateway
	// (e.g. ":10000"). When empty the gateway is disabled.
	GatewayListen string `yaml:"gateway_listen"`
}

// CheckURLItem is one health-check target URL. A proxy is considered healthy
// when it can reach ANY enabled check URL, so listing several targets lets the
// same proxy be validated against different sites (e.g. domestic + overseas).
// Enabled defaults to true when omitted.
type CheckURLItem struct {
	Name    string `yaml:"name" json:"name"`
	URL     string `yaml:"url" json:"url"`
	Enabled *bool  `yaml:"enabled" json:"enabled"`
}

// IsEnabled reports whether the check URL participates in health checks.
// A nil Enabled value (field omitted) means enabled.
func (i CheckURLItem) IsEnabled() bool {
	return i.Enabled == nil || *i.Enabled
}

// EnabledCheckURLs returns the enabled, non-blank check URLs of a list.
func EnabledCheckURLs(items []CheckURLItem) []string {
	var out []string
	for _, it := range items {
		if !it.IsEnabled() {
			continue
		}
		if u := strings.TrimSpace(it.URL); u != "" {
			out = append(out, u)
		}
	}
	return out
}

type HealthConfig struct {
	IntervalSeconds int    `yaml:"interval_s"`
	TimeoutMs       int    `yaml:"timeout_ms"`
	Concurrency     int    `yaml:"concurrency"`
	CheckURL        string `yaml:"check_url"`
	// CheckURLs is the list of default health-check target URLs. When a
	// provider does not configure its own check URLs, the enabled defaults are
	// used. All defaults are enabled unless marked otherwise.
	CheckURLs         []CheckURLItem `yaml:"check_urls"`
	MaxFails          int            `yaml:"max_fails"`
	RebuildIntervalMs int            `yaml:"rebuild_interval_ms"`
}

type ProviderCfg struct {
	Name     string `yaml:"name" json:"name"`
	Type     string `yaml:"type" json:"type"`
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Weight   int    `yaml:"weight" json:"weight"`
	Priority int    `yaml:"priority" json:"priority"`
	// CheckIntervalS 检测频率：池内 IP 多久检测一次延迟（秒）。0=用全局 health_check.interval_s。
	CheckIntervalS int     `yaml:"check_interval_s" json:"check_interval_s"`
	MinAliveRatio  float64 `yaml:"min_alive_ratio" json:"min_alive_ratio"`
	StickySeconds  int     `yaml:"sticky_seconds" json:"sticky_seconds"`
	CheckURL       string  `yaml:"check_url" json:"check_url"`
	// CheckURLs 自定义健康检测测试 URL 列表（可多个，每个可独立启用/停用）。
	// 任一启用的 URL 可达即判定代理存活；未配置时回退到 check_url 或全局 health_check.check_urls。
	// 新增/编辑 Provider 的界面会把 5 个全局默认测试 URL（含启用状态）与自定义 URL 一起快照到这里。
	CheckURLs []CheckURLItem `yaml:"check_urls" json:"check_urls"`
	// Country 可选：显式指定该 provider 所有代理的国家代码（ISO alpha-2），
	// 入池后直接打标，跳过 IP 归属查询。
	Country string          `yaml:"country" json:"country"`
	Tunnel  *TunnelConfig   `yaml:"tunnel" json:"tunnel"`
	IPPool  *IPPoolConfig   `yaml:"ip_pool" json:"ip_pool"`
	Free    *FreePoolConfig `yaml:"free" json:"free"`
	// Owner 归属账户名：空=全局（由 admin 创建，所有用户可见可用）；非空=私有，
	// 仅 owner 本人与 admin 可查看、编辑、删除。
	Owner string `yaml:"owner" json:"owner"`
	// Public 公开共享开关：仅对私有 Provider（Owner 非空）有意义。开启后其他
	// 用户可见并可使用该 Provider 的代理；关闭后仅 owner 可见。全局 Provider
	// 恒为公开。
	Public bool `yaml:"public" json:"public"`
}

type TunnelConfig struct {
	Gateway  string `yaml:"gateway" json:"gateway"`
	Port     int    `yaml:"port" json:"port"`
	Scheme   string `yaml:"scheme" json:"scheme"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

type IPPoolConfig struct {
	ExtractURL     string `yaml:"extract_url" json:"extract_url"`
	RefreshSeconds int    `yaml:"refresh_interval_s" json:"refresh_interval_s"`
	ExpireSeconds  int    `yaml:"expire_after_s" json:"expire_after_s"`
	Username       string `yaml:"username" json:"username"`
	Password       string `yaml:"password" json:"password"`
	Format         string `yaml:"format" json:"format"`
	MaxProxies     int    `yaml:"max_proxies" json:"max_proxies"`
}

// FreePoolConfig configures a free-proxy source: a JSON feed listing free
// proxies with a reported speed in milliseconds. Only proxies whose reported
// speed is below MaxSpeedMS are collected, and every collected proxy is marked
// Free (fail-once-delete semantics in health checks).
type FreePoolConfig struct {
	FeedURL         string `yaml:"feed_url" json:"feed_url"`
	RefreshSeconds  int    `yaml:"refresh_interval_s" json:"refresh_interval_s"`
	ExpireSeconds   int    `yaml:"expire_after_s" json:"expire_after_s"`
	MaxProxies      int    `yaml:"max_proxies" json:"max_proxies"`
	MaxSpeedMS      int    `yaml:"max_speed_ms" json:"max_speed_ms"`           // 只采集上报延迟低于该值的代理；默认 3000 (3s)
	DeleteLatencyMS int    `yaml:"delete_latency_ms" json:"delete_latency_ms"` // 健康检测延迟超过该值直接删除；默认 3000 (3s)
}

// EnabledCheckURLs returns this provider's enabled custom health-check URLs.
func (p ProviderCfg) EnabledCheckURLs() []string {
	return EnabledCheckURLs(p.CheckURLs)
}

// EnabledCheckURLs returns the enabled default health-check URLs.
func (h HealthConfig) EnabledCheckURLs() []string {
	return EnabledCheckURLs(h.CheckURLs)
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	applyDefaults(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":8080"
	}
	if cfg.Server.ReadTimeout == 0 {
		cfg.Server.ReadTimeout = 3000
	}
	if cfg.Server.WriteTimeout == 0 {
		cfg.Server.WriteTimeout = 3000
	}
	if cfg.Server.MaxWorkers == 0 {
		cfg.Server.MaxWorkers = 4096
	}
	if cfg.HealthCheck.IntervalSeconds == 0 {
		cfg.HealthCheck.IntervalSeconds = 30
	}
	if cfg.HealthCheck.TimeoutMs == 0 {
		cfg.HealthCheck.TimeoutMs = 5000
	}
	if cfg.HealthCheck.Concurrency == 0 {
		cfg.HealthCheck.Concurrency = 64
	}
	if cfg.HealthCheck.CheckURL == "" {
		cfg.HealthCheck.CheckURL = "http://httpbin.org/ip"
	}
	if len(cfg.HealthCheck.CheckURLs) == 0 {
		cfg.HealthCheck.CheckURLs = defaultCheckURLs()
	}
	if cfg.HealthCheck.MaxFails == 0 {
		cfg.HealthCheck.MaxFails = 3
	}
	if cfg.HealthCheck.RebuildIntervalMs == 0 {
		cfg.HealthCheck.RebuildIntervalMs = 200
	}
	if cfg.Alerts.DedupSeconds == 0 {
		cfg.Alerts.DedupSeconds = 300
	}
	if cfg.Alerts.MonitorSeconds == 0 {
		cfg.Alerts.MonitorSeconds = 15
	}
	if cfg.Alerts.RecoverSeconds == 0 {
		cfg.Alerts.RecoverSeconds = 3600
	}
	if cfg.Alerts.Email.SMTPPort == 0 {
		cfg.Alerts.Email.SMTPPort = 587
	}
	if cfg.DB.BatchSize == 0 {
		cfg.DB.BatchSize = 200
	}
	if cfg.DB.FlushIntervalMs == 0 {
		cfg.DB.FlushIntervalMs = 1000
	}
	if cfg.Geo.IntervalSecs == 0 {
		cfg.Geo.IntervalSecs = 60
	}
	if cfg.Geo.URL == "" {
		cfg.Geo.URL = "http://ip-api.com/batch"
	}
}

// defaultCheckURLs returns the 5 built-in default health-check targets used
// when health_check.check_urls is not configured. These are lightweight IP
// echo / country-code endpoints that return very little data (a few bytes),
// keeping proxy traffic usage minimal. ip-api.com also reports the country
// code of the proxy's exit IP, letting the checker refresh a proxy's country
// for free. All are enabled by default; they can be overridden in yaml or
// per-provider in the web console.
func defaultCheckURLs() []CheckURLItem {
	t := true
	return []CheckURLItem{
		{Name: "ipapi-cc", URL: "http://ip-api.com/line/?fields=countryCode", Enabled: &t},
		{Name: "ipapi-json", URL: "http://ip-api.com/json/?fields=countryCode,query", Enabled: &t},
		{Name: "ipify", URL: "https://api.ipify.org", Enabled: &t},
		{Name: "ipsb", URL: "https://ip.sb", Enabled: &t},
		{Name: "icanhazip", URL: "https://icanhazip.com", Enabled: &t},
	}
}

type UnsupportedTypeError struct {
	Type string
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("unsupported provider type %q", e.Type)
}

// validRegion reports whether a group region filter value is legal:
// "domestic" (CN 中国内地), "overseas" (非中国内地，含中国香港) or an ISO
// alpha-2 country code such as "US" / "JP" / "HK".
func validRegion(r string) bool {
	switch r {
	case "domestic", "overseas":
		return true
	}
	return isISOAlpha2(r)
}

// ValidRegion is the exported form of validRegion, used by the admin API to
// validate group region filters supplied at runtime.
func ValidRegion(r string) bool {
	return validRegion(r)
}

// isISOAlpha2 reports whether s is a two-letter ASCII code (upper or lower).
func isISOAlpha2(s string) bool {
	if len(s) != 2 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	return true
}

// Provider returns the provider config by name.
func (c *Config) Provider(name string) (ProviderCfg, bool) {
	for _, p := range c.Providers {
		if p.Name == name {
			return p, true
		}
	}
	return ProviderCfg{}, false
}

// Group returns the group config by name.
func (c *Config) Group(name string) (GroupCfg, bool) {
	for _, g := range c.Groups {
		if g.Name == name {
			return g, true
		}
	}
	return GroupCfg{}, false
}

func (c *Config) Validate() error {
	if len(c.Providers) == 0 {
		return fmt.Errorf("at least one provider is required")
	}
	for i, it := range c.HealthCheck.CheckURLs {
		if strings.TrimSpace(it.URL) == "" {
			return fmt.Errorf("health_check.check_urls[%d]: url is required", i)
		}
	}
	provNames := map[string]bool{}
	for i := range c.Providers {
		p := &c.Providers[i]
		if p.Name == "" {
			return fmt.Errorf("providers[%d]: name is required", i)
		}
		if provNames[p.Name] {
			return fmt.Errorf("providers[%s]: duplicate name", p.Name)
		}
		provNames[p.Name] = true
		switch p.Type {
		case "tunnel", "ip_pool", "sticky", "free":
		default:
			return fmt.Errorf("providers[%s]: unsupported type %q (want tunnel, ip_pool, sticky or free)", p.Name, p.Type)
		}
		if p.Type == "tunnel" {
			if p.Tunnel == nil {
				return fmt.Errorf("providers[%s]: tunnel config is required", p.Name)
			}
			if p.Tunnel.Gateway == "" {
				return fmt.Errorf("providers[%s]: tunnel.gateway is required", p.Name)
			}
			if p.Tunnel.Port == 0 {
				p.Tunnel.Port = 8080
			}
			if p.Tunnel.Scheme == "" {
				p.Tunnel.Scheme = "http"
			}
		}
		if p.Type == "ip_pool" || p.Type == "sticky" {
			if p.IPPool == nil {
				return fmt.Errorf("providers[%s]: ip_pool config is required", p.Name)
			}
			if p.IPPool.ExtractURL == "" {
				return fmt.Errorf("providers[%s]: ip_pool.extract_url is required", p.Name)
			}
			if p.IPPool.Format == "" {
				p.IPPool.Format = "raw"
			}
			if p.IPPool.RefreshSeconds == 0 {
				p.IPPool.RefreshSeconds = 60
			}
			if p.IPPool.ExpireSeconds == 0 {
				p.IPPool.ExpireSeconds = 600
			}
		}
		if p.Type == "sticky" && p.StickySeconds == 0 {
			p.StickySeconds = 60
		}
		if p.Type == "free" {
			if p.Free == nil {
				return fmt.Errorf("providers[%s]: free config is required", p.Name)
			}
			if p.Free.FeedURL == "" {
				return fmt.Errorf("providers[%s]: free.feed_url is required", p.Name)
			}
			if p.Free.MaxSpeedMS == 0 {
				p.Free.MaxSpeedMS = 3000
			}
			if p.Free.DeleteLatencyMS == 0 {
				p.Free.DeleteLatencyMS = 3000
			}
			if p.Free.RefreshSeconds == 0 {
				p.Free.RefreshSeconds = 60
			}
			if p.CheckURL == "" && len(p.CheckURLs) == 0 {
				return fmt.Errorf("providers[%s]: check_url or check_urls is required for free proxies", p.Name)
			}
		}
		for _, it := range p.CheckURLs {
			if strings.TrimSpace(it.URL) == "" {
				return fmt.Errorf("providers[%s]: check_urls[%s] url is required", p.Name, it.Name)
			}
		}
		if p.Weight == 0 {
			p.Weight = 1
		}
		if p.Country != "" && !isISOAlpha2(p.Country) {
			return fmt.Errorf("providers[%s]: country %q is not a valid ISO alpha-2 code", p.Name, p.Country)
		}
		if p.MinAliveRatio < 0 {
			return fmt.Errorf("providers[%s]: min_alive_ratio must be >= 0", p.Name)
		}
		if p.MinAliveRatio > 1 {
			return fmt.Errorf("providers[%s]: min_alive_ratio must be <= 1", p.Name)
		}
	}
	if len(c.Groups) > 0 {
		groupNames := map[string]bool{}
		for gi := range c.Groups {
			g := &c.Groups[gi]
			if g.Name == "" {
				return fmt.Errorf("groups[%d]: name is required", gi)
			}
			if groupNames[g.Name] {
				return fmt.Errorf("groups[%s]: duplicate name", g.Name)
			}
			groupNames[g.Name] = true
			if g.MinAliveRatio < 0 || g.MinAliveRatio > 1 {
				return fmt.Errorf("groups[%s]: min_alive_ratio must be in [0,1]", g.Name)
			}
			for _, r := range g.Regions {
				if !validRegion(r) {
					return fmt.Errorf("groups[%s]: invalid region %q (want domestic, overseas or ISO alpha-2 code)", g.Name, r)
				}
			}
			for _, pn := range g.Primary {
				if !provNames[pn] {
					return fmt.Errorf("groups[%s]: primary references unknown provider %q", g.Name, pn)
				}
			}
			for pn, w := range g.PrimaryWeights {
				if w < 1 {
					return fmt.Errorf("groups[%s]: primary_weights[%s] must be >= 1", g.Name, pn)
				}
				found := false
				for _, prim := range g.Primary {
					if prim == pn {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("groups[%s]: primary_weights references %q which is not in primary", g.Name, pn)
				}
			}
			for bi := range g.Backups {
				b := &g.Backups[bi]
				if b.Name == "" {
					return fmt.Errorf("groups[%s].backups[%d]: name is required", g.Name, bi)
				}
				if b.MinAliveRatio < 0 || b.MinAliveRatio > 1 {
					return fmt.Errorf("groups[%s].backups[%s]: min_alive_ratio must be in [0,1]", g.Name, b.Name)
				}
				for _, pn := range b.Providers {
					if !provNames[pn] {
						return fmt.Errorf("groups[%s].backups[%s]: references unknown provider %q", g.Name, b.Name, pn)
					}
				}
			}
		}
	}
	acctNames := map[string]bool{}
	tokens := map[string]bool{}
	for i := range c.Accounts {
		a := &c.Accounts[i]
		if a.Name == "" {
			return fmt.Errorf("accounts[%d]: name is required", i)
		}
		if acctNames[a.Name] {
			return fmt.Errorf("accounts[%s]: duplicate name", a.Name)
		}
		acctNames[a.Name] = true
		if a.Token == "" {
			return fmt.Errorf("accounts[%s]: token is required", a.Name)
		}
		if tokens[a.Token] {
			return fmt.Errorf("accounts[%s]: duplicate token", a.Name)
		}
		tokens[a.Token] = true
		if a.Role != "admin" && a.Role != "user" && a.Role != "" {
			return fmt.Errorf("accounts[%s]: role must be admin or user", a.Name)
		}
	}
	return nil
}
