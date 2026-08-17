package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

// FreePool collects free proxies from a public JSON feed (e.g.
// https://charlespikachu.github.io/freeproxy/proxies.json). Only proxies whose
// reported speed is below MaxSpeedMS are kept, and every proxy is marked Free
// so health checks delete it on the first failure (or when measured latency
// exceeds the configured delete threshold).
type FreePool struct {
	cfg config.ProviderCfg
	hc  *http.Client
}

func NewFreePool(cfg config.ProviderCfg) *FreePool {
	return &FreePool{
		cfg: cfg,
		hc: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (p *FreePool) Name() string { return p.cfg.Name }
func (p *FreePool) Kind() model.Kind {
	return model.KindIPPool
}

func (p *FreePool) Weight() int32 {
	return int32(p.cfg.Weight)
}

func (p *FreePool) CheckURL() string {
	if p.cfg.CheckURL != "" {
		return p.cfg.CheckURL
	}
	return ""
}

func (p *FreePool) Initial(ctx context.Context) ([]*model.Proxy, error) {
	return p.Refresh(ctx)
}

type freeProxyEntry struct {
	IP        string `json:"ip"`
	Port      int    `json:"port"`
	Protocol  string `json:"protocol"`
	Country   string `json:"country"`
	Anonymity string `json:"anonymity"`
	Speed     int    `json:"speed"` // milliseconds
}

func (p *FreePool) Refresh(ctx context.Context) ([]*model.Proxy, error) {
	fc := p.cfg.Free
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fc.FeedURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("free feed %s returned %d: %s", fc.FeedURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}

	return p.parse(body), nil
}

// parse extracts free HTTP/HTTPS proxies whose reported speed is below the
// configured threshold. Entries that only support SOCKS are skipped because
// the health checker performs an HTTP CONNECT probe.
func (p *FreePool) parse(body []byte) []*model.Proxy {
	fc := p.cfg.Free
	var feed struct {
		Data []freeProxyEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &feed); err != nil {
		return nil
	}

	maxSpeed := fc.MaxSpeedMS
	if maxSpeed <= 0 {
		maxSpeed = 2000
	}
	expire := int64(fc.ExpireSeconds) // 0 = 不过期（只要代理还能用、延迟达标就一直保留）

	now := time.Now()
	proxies := make([]*model.Proxy, 0, len(feed.Data))
	seen := make(map[string]struct{}, len(feed.Data))
	for _, d := range feed.Data {
		if fc.MaxProxies > 0 && len(proxies) >= fc.MaxProxies {
			break
		}
		if d.Speed <= 0 || d.Speed >= maxSpeed {
			continue
		}
		if !supportsHTTP(d.Protocol) {
			continue
		}
		if d.Port <= 0 || d.Port > 65535 {
			continue
		}
		host := strings.TrimSpace(d.IP)
		if host == "" {
			continue
		}
		key := host + ":" + strconv.Itoa(d.Port)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		pr := &model.Proxy{
			ID:              fmt.Sprintf("%s:%s", p.cfg.Name, key),
			Provider:        p.cfg.Name,
			Kind:            model.KindIPPool,
			Scheme:          "http",
			Host:            host,
			Port:            d.Port,
			Weight:          int32(p.cfg.Weight),
			CheckURL:        p.CheckURL(),
			CheckIntervalMS: int64(p.cfg.CheckIntervalS) * 1000,
			Priority:        p.cfg.Priority,
			MinAliveRatio:   p.cfg.MinAliveRatio,
			Free:            true,
			DeleteLatencyMS: int64(fc.DeleteLatencyMS),
		}
		pr.Alive.Store(true)
		if expire > 0 {
			pr.ExpireAt = now.Add(time.Duration(expire) * time.Second).UnixNano()
		}
		proxies = append(proxies, pr)
	}
	return proxies
}

// supportsHTTP reports whether the protocol string advertises HTTP(S) proxying.
func supportsHTTP(protocol string) bool {
	lower := strings.ToLower(protocol)
	return strings.Contains(lower, "http")
}
