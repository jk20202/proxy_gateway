package config

import "testing"

func TestLoadFreeConfig(t *testing.T) {
	cfg, err := Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.HealthCheck.CheckURLs) != 5 {
		t.Fatalf("config.yaml should define 5 default check_urls, got %d", len(cfg.HealthCheck.CheckURLs))
	}
	if urls := cfg.HealthCheck.EnabledCheckURLs(); len(urls) != 5 {
		t.Fatalf("all 5 default check_urls should be enabled, got %d: %v", len(urls), urls)
	}
	if !cfg.FreeAPI.Enabled || cfg.FreeAPI.FeedURL == "" || cfg.FreeAPI.MaxSpeedMS <= 0 {
		t.Fatalf("config.yaml should enable free_api with feed_url and max_speed_ms: %+v", cfg.FreeAPI)
	}
	found := false
	for _, p := range cfg.Providers {
		if p.Name == "free-proxies" {
			found = true
			if p.Type != "ip_pool" || p.IPPool == nil {
				t.Fatalf("free-proxies should be ip_pool type: %+v", p)
			}
			if p.IPPool.ExtractURL != "http://127.0.0.1:8080/api/v1/free-proxies" {
				t.Fatalf("free-proxies extract_url should point at the free_api endpoint: %+v", p.IPPool)
			}
		}
	}
	if !found {
		t.Fatal("free-proxies provider not found")
	}
	for _, p := range cfg.Providers {
		if p.Type == "free" {
			t.Fatalf("no provider should use the local free crawler type anymore: %+v", p)
		}
	}
}

func TestEnabledCheckURLs(t *testing.T) {
	tru, fls := true, false
	items := []CheckURLItem{
		{Name: "a", URL: "http://a/", Enabled: &tru},
		{Name: "b", URL: "http://b/", Enabled: &fls},
		{Name: "c", URL: "  http://c/  ", Enabled: nil},
		{Name: "d", URL: "   ", Enabled: &tru},
	}
	got := EnabledCheckURLs(items)
	if len(got) != 2 || got[0] != "http://a/" || got[1] != "http://c/" {
		t.Fatalf("EnabledCheckURLs = %v, want [http://a/ http://c/]", got)
	}
}
