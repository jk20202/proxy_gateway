package health

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
	"proxy-pool/internal/pool"
)

type Checker struct {
	cfg     config.HealthConfig
	pool    *pool.Pool
	logger  *slog.Logger
	timeout time.Duration

	lastCheckMu sync.Mutex
	lastCheck   map[string]time.Time // proxyID -> 上次实际检测时间
}

func NewChecker(cfg config.HealthConfig, p *pool.Pool, logger *slog.Logger) *Checker {
	return &Checker{
		cfg:       cfg,
		pool:      p,
		logger:    logger,
		timeout:   time.Duration(cfg.TimeoutMs) * time.Millisecond,
		lastCheck: make(map[string]time.Time),
	}
}

func (c *Checker) Run(ctx context.Context) {
	interval := c.minInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	c.checkAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkDue(ctx)
			if d := c.minInterval(); d != interval {
				interval = d
				ticker.Reset(d)
			}
		}
	}
}

// minInterval returns the shortest effective check interval across all
// proxies, honoring per-provider check_interval_s. Falls back to the global
// health_check.interval_s when no proxy sets a custom interval.
func (c *Checker) minInterval() time.Duration {
	min := int64(c.cfg.IntervalSeconds) * 1000
	if min <= 0 {
		min = 60_000
	}
	for _, pr := range c.pool.All() {
		if pr.CheckIntervalMS > 0 && pr.CheckIntervalMS < min {
			min = pr.CheckIntervalMS
		}
	}
	if min < 1000 {
		min = 1000 // never busy-loop faster than 1s
	}
	return time.Duration(min) * time.Millisecond
}

func (c *Checker) CheckOnce(ctx context.Context) {
	c.checkAll(ctx)
}

// checkDue probes only the proxies whose check interval has elapsed since
// their last actual probe. Proxies without a custom interval use the global
// health_check.interval_s.
func (c *Checker) checkDue(ctx context.Context) {
	proxies := c.pool.All()
	if len(proxies) == 0 {
		return
	}
	now := time.Now()
	due := make([]*model.Proxy, 0, len(proxies))
	c.lastCheckMu.Lock()
	for _, pr := range proxies {
		interval := pr.CheckIntervalMS
		if interval <= 0 {
			interval = int64(c.cfg.IntervalSeconds) * 1000
		}
		if interval <= 0 {
			interval = 60_000
		}
		last, ok := c.lastCheck[pr.ID]
		if !ok || now.Sub(last) >= time.Duration(interval)*time.Millisecond {
			due = append(due, pr)
			c.lastCheck[pr.ID] = now
		}
	}
	c.lastCheckMu.Unlock()

	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, pr := range due {
		pr := pr
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.checkOne(ctx, pr)
		}()
	}
	wg.Wait()
}

func (c *Checker) checkAll(ctx context.Context) {
	proxies := c.pool.All()
	if len(proxies) == 0 {
		return
	}

	c.lastCheckMu.Lock()
	for _, pr := range proxies {
		c.lastCheck[pr.ID] = time.Now()
	}
	c.lastCheckMu.Unlock()

	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, pr := range proxies {
		pr := pr
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.checkOne(ctx, pr)
		}()
	}
	wg.Wait()
}

func (c *Checker) checkOne(ctx context.Context, pr *model.Proxy) {
	checkURLs := c.resolveCheckURLs(pr)
	if len(checkURLs) == 0 {
		return
	}
	start := time.Now()
	ok, country := c.probe(ctx, pr, checkURLs)
	latency := time.Since(start)

	if ok {
		if country != "" && !isAlpha2(pr.Country) {
			// 从 IP-echo 端点检测到代理出口 IP 的国家代码，顺带刷新。
			// 未检测到国家（country==""）时保留已有国家，避免用空值覆盖上一轮结果。
			c.pool.SetCountry(pr.ID, country)
		}
		c.pool.MarkSuccess(pr.ID, latency.Milliseconds())
		if pr.Free && latency.Milliseconds() > deleteThreshold(pr) {
			// 免费代理延迟超过阈值：直接删除，不做轮换保留
			if c.pool.Remove(pr.ID) {
				c.logger.Warn("free proxy removed: latency too high", "proxy", pr.ID, "latency_ms", latency.Milliseconds())
			}
		}
		return
	}

	if pr.Free {
		// 免费代理一次检测失败即删除，不累积失败次数
		if c.pool.Remove(pr.ID) {
			c.logger.Warn("free proxy removed: check failed", "proxy", pr.ID, "latency_ms", latency.Milliseconds())
		}
		return
	}
	c.pool.MarkFailed(pr.ID, latency.Milliseconds())
}

// DefaultCheckURLs returns the globally configured default health-check
// target URLs (with their enabled state), used by the admin API to render the
// provider test-URL editor in the web console.
func (c *Checker) DefaultCheckURLs() []config.CheckURLItem {
	return c.cfg.CheckURLs
}

// resolveCheckURLs computes the effective health-check URL list for a proxy:
// provider-specific URLs first, then the single legacy check_url, then the
// global defaults, then the legacy global check_url.
func (c *Checker) resolveCheckURLs(pr *model.Proxy) []string {
	if len(pr.CheckURLs) > 0 {
		return pr.CheckURLs
	}
	if pr.CheckURL != "" {
		return []string{pr.CheckURL}
	}
	if urls := c.cfg.EnabledCheckURLs(); len(urls) > 0 {
		return urls
	}
	if c.cfg.CheckURL != "" {
		return []string{c.cfg.CheckURL}
	}
	return nil
}

// deleteThreshold returns the latency threshold above which a free proxy is
// deleted. It prefers the provider's configured value; falls back to 3000ms.
func deleteThreshold(pr *model.Proxy) int64 {
	if pr.DeleteLatencyMS > 0 {
		return pr.DeleteLatencyMS
	}
	return 3000
}

// probe reports whether the proxy can reach ANY of the given check URLs and,
// when the successful response carries a country code (e.g. from an IP-echo
// endpoint), the detected country of the proxy's exit IP. All URLs are probed
// in parallel sharing one client; the first success cancels the remaining
// in-flight probes.
func (c *Checker) probe(ctx context.Context, pr *model.Proxy, checkURLs []string) (bool, string) {
	if len(checkURLs) == 0 {
		return false, ""
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	proxyURL := &url.URL{
		Scheme: pr.Scheme,
		Host:   pr.Addr(),
	}
	if pr.Username != "" {
		proxyURL.User = url.UserPassword(pr.Username, pr.Password)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout: c.timeout,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          100,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   c.timeout,
		ResponseHeaderTimeout: c.timeout,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   c.timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	type probeResult struct {
		ok      bool
		country string
	}
	result := make(chan probeResult, len(checkURLs))
	var wg sync.WaitGroup
	for _, checkURL := range checkURLs {
		checkURL := checkURL
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, country := c.probeOne(ctx, client, checkURL)
			if ok {
				select {
				case result <- probeResult{ok: true, country: country}:
					if country != "" {
						cancel() // country already detected: stop remaining probes
					}
				default:
				}
			}
		}()
	}
	wg.Wait()
	// Prefer a successful result that also detected a country so the proxy's
	// country can be refreshed; otherwise any success is enough for liveness.
	anyOK := false
	country := ""
	close(result)
	for r := range result {
		if !r.ok {
			continue
		}
		anyOK = true
		if country == "" && r.country != "" {
			country = r.country
		}
	}
	return anyOK, country
}

func (c *Checker) probeOne(ctx context.Context, client *http.Client, checkURL string) (bool, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
	if err != nil {
		return false, ""
	}
	req.Header.Set("User-Agent", "proxypool-health/1.0")
	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	if err != nil {
		return false, ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return false, ""
	}
	// Read a bounded amount of the body: IP-echo endpoints return only a few
	// bytes (e.g. "HK" or a bare IP), so 1KB is more than enough to detect a
	// country code without wasting the proxy's traffic.
	body := make([]byte, 1024)
	n, _ := io.ReadFull(resp.Body, body)
	return true, parseCountry(body[:n])
}

// parseCountry extracts an ISO-3166 alpha-2 country code from a short response
// body produced by common IP-echo / country endpoints. Recognized formats:
//
//	"HK"                         (ip-api.com/line?fields=countryCode)
//	{"countryCode":"HK", ...}    (ip-api.com/json?fields=countryCode,query)
//
// Endpoints that return only a bare IP (api.ipify.org, ip.sb, icanhazip.com)
// yield "" so the proxy's previously detected country is preserved.
func parseCountry(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return ""
	}
	// JSON: {"countryCode":"HK", ...}
	if len(trimmed) > 2 && trimmed[0] == '{' {
		var v struct {
			CountryCode string `json:"countryCode"`
		}
		if err := json.Unmarshal(trimmed, &v); err == nil && v.CountryCode != "" {
			if isAlpha2(v.CountryCode) {
				return strings.ToUpper(v.CountryCode)
			}
		}
		return ""
	}
	// Plain 2-letter code: "HK"
	if isAlpha2(string(trimmed)) {
		return strings.ToUpper(string(trimmed))
	}
	return ""
}

func isAlpha2(s string) bool {
	if len(s) != 2 {
		return false
	}
	for i := 0; i < 2; i++ {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}
