package provider

import (
	"bufio"
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

type IPPool struct {
	cfg config.ProviderCfg
	hc  *http.Client
}

func NewIPPool(cfg config.ProviderCfg) *IPPool {
	return &IPPool{
		cfg: cfg,
		hc: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (p *IPPool) Name() string { return p.cfg.Name }
func (p *IPPool) Kind() model.Kind {
	if p.cfg.Type == "sticky" {
		return model.KindSticky
	}
	return model.KindIPPool
}

func (p *IPPool) Weight() int32 {
	return int32(p.cfg.Weight)
}

func (p *IPPool) CheckURL() string {
	if p.cfg.CheckURL != "" {
		return p.cfg.CheckURL
	}
	return ""
}

func (p *IPPool) Initial(ctx context.Context) ([]*model.Proxy, error) {
	return p.Refresh(ctx)
}

func (p *IPPool) Refresh(ctx context.Context) ([]*model.Proxy, error) {
	ipc := p.cfg.IPPool
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ipc.ExtractURL, nil)
	if err != nil {
		return nil, err
	}
	if ipc.Username != "" {
		req.SetBasicAuth(ipc.Username, ipc.Password)
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("extract api %s returned %d: %s", ipc.ExtractURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	proxies := p.parse(body)
	return proxies, nil
}

func (p *IPPool) parse(body []byte) []*model.Proxy {
	ipc := p.cfg.IPPool
	var entries []string

	switch ipc.Format {
	case "json":
		var raw struct {
			Data []struct {
				IP   string `json:"ip"`
				Port int    `json:"port"`
				Addr string `json:"addr"`
			} `json:"data"`
			DataArr []string `json:"data_list"`
			List    []string `json:"list"`
			IPs     []string `json:"ips"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil
		}
		for _, d := range raw.Data {
			if d.Addr != "" {
				entries = append(entries, d.Addr)
			} else if d.IP != "" && d.Port > 0 {
				entries = append(entries, fmt.Sprintf("%s:%d", d.IP, d.Port))
			}
		}
		entries = append(entries, raw.DataArr...)
		entries = append(entries, raw.List...)
		entries = append(entries, raw.IPs...)
	case "text", "raw", "":
		sc := bufio.NewScanner(strings.NewReader(string(body)))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line != "" {
				entries = append(entries, line)
			}
		}
	default:
		return nil
	}

	expire := int64(ipc.ExpireSeconds)
	if expire == 0 {
		expire = 600
	}

	now := time.Now()
	proxies := make([]*model.Proxy, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if ipc.MaxProxies > 0 && len(proxies) >= ipc.MaxProxies {
			break
		}
		host, portStr, ok := splitAddr(e)
		if !ok {
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			continue
		}
		if _, dup := seen[host+":"+portStr]; dup {
			continue
		}
		seen[host+":"+portStr] = struct{}{}
		kind := model.KindIPPool
		if p.cfg.Type == "sticky" {
			kind = model.KindSticky
		}
		pr := &model.Proxy{
			ID:              fmt.Sprintf("%s:%s:%d", p.cfg.Name, host, port),
			Provider:        p.cfg.Name,
			Kind:            kind,
			Scheme:          "http",
			Host:            host,
			Port:            port,
			Weight:          int32(p.cfg.Weight),
			Username:        ipc.Username,
			Password:        ipc.Password,
			CheckURL:        p.CheckURL(),
			CheckIntervalMS: int64(p.cfg.CheckIntervalS) * 1000,
			Priority:        p.cfg.Priority,
			MinAliveRatio:   p.cfg.MinAliveRatio,
			StickySeconds:   p.cfg.StickySeconds,
		}
		pr.Alive.Store(true)
		pr.ExpireAt = now.Add(time.Duration(expire) * time.Second).UnixNano()
		proxies = append(proxies, pr)
	}
	return proxies
}

func splitAddr(addr string) (host, port string, ok bool) {
	addr = strings.TrimSpace(addr)
	if strings.Contains(addr, "://") {
		idx := strings.Index(addr, "://")
		addr = addr[idx+3:]
	}
	if i := strings.LastIndex(addr, ":"); i > 0 {
		return addr[:i], addr[i+1:], true
	}
	return "", "", false
}
