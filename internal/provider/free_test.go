package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxy-pool/internal/config"
)

func testFreeCfg() config.ProviderCfg {
	return config.ProviderCfg{
		Name:     "freeproxy",
		Type:     "free",
		Enabled:  true,
		Weight:   10,
		CheckURL: "https://httpbin.org/ip",
		Free: &config.FreePoolConfig{
			FeedURL:         "http://127.0.0.1/feed.json",
			RefreshSeconds:  60,
			ExpireSeconds:   0,
			MaxProxies:      0,
			MaxSpeedMS:      3000,
			DeleteLatencyMS: 3000,
		},
	}
}

func TestFreePoolParseFilters(t *testing.T) {
	body := `{"data":[
		{"ip":"1.1.1.1","port":8080,"protocol":"Http","speed":300},
		{"ip":"2.2.2.2","port":80,"protocol":"Http, Https","speed":1500},
		{"ip":"3.3.3.3","port":1080,"protocol":"Socks5","speed":100},
		{"ip":"4.4.4.4","port":99999,"protocol":"Http","speed":500},
		{"ip":"5.5.5.5","port":8080,"protocol":"Http","speed":2500},
		{"ip":"6.6.6.6","port":3128,"protocol":"Http","speed":0},
		{"ip":"7.7.7.7","port":8080,"protocol":"Http","speed":2900},
		{"ip":"8.8.8.8","port":8080,"protocol":"Http","speed":3200}
	]}`
	fp := NewFreePool(testFreeCfg())
	proxies := fp.parse([]byte(body))
	if len(proxies) != 4 {
		t.Fatalf("want 4 proxies (http + <3s), got %d", len(proxies))
	}
	for _, pr := range proxies {
		if !pr.Free {
			t.Fatalf("proxy %s not marked free", pr.ID)
		}
		if pr.DeleteLatencyMS != 3000 {
			t.Fatalf("proxy %s delete latency = %d", pr.ID, pr.DeleteLatencyMS)
		}
		if pr.ExpireAt != 0 {
			t.Fatalf("proxy %s expire_at = %d, want 0 (never expire)", pr.ID, pr.ExpireAt)
		}
	}
}

func TestFreePoolRefresh(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":[{"ip":"7.7.7.7","port":8080,"protocol":"Http","speed":800}]}`))
	}))
	defer srv.Close()

	cfg := testFreeCfg()
	cfg.Free.FeedURL = srv.URL
	fp := NewFreePool(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proxies, err := fp.Refresh(ctx)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if len(proxies) != 1 || proxies[0].Host != "7.7.7.7" {
		t.Fatalf("unexpected proxies: %+v", proxies)
	}
	if proxies[0].Kind.String() != "ip_pool" {
		t.Fatalf("free proxy kind = %s, want ip_pool", proxies[0].Kind.String())
	}
}
