package health

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	checkURL := pr.CheckURL
	if checkURL == "" {
		checkURL = c.cfg.CheckURL
	}
	start := time.Now()
	ok := c.probe(ctx, pr, checkURL)
	latency := time.Since(start)

	if ok {
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

// deleteThreshold returns the latency threshold above which a free proxy is
// deleted. It prefers the provider's configured value; falls back to 3000ms.
func deleteThreshold(pr *model.Proxy) int64 {
	if pr.DeleteLatencyMS > 0 {
		return pr.DeleteLatencyMS
	}
	return 3000
}

func (c *Checker) probe(ctx context.Context, pr *model.Proxy, checkURL string) bool {
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "proxypool-health/1.0")
	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
