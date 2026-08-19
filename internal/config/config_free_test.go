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
	found := false
	for _, p := range cfg.Providers {
		if p.Name == "charlespikachu-free" {
			found = true
			if p.Type != "free" || p.Free == nil {
				t.Fatalf("charlespikachu-free not free type: %+v", p)
			}
			if p.Free.FeedURL == "" || p.Free.MaxSpeedMS != 3000 || p.Free.DeleteLatencyMS != 3000 {
				t.Fatalf("charlespikachu-free free cfg wrong: %+v", p.Free)
			}
			if p.Free.ExpireSeconds != 0 || p.Free.MaxProxies != 0 {
				t.Fatalf("free provider must not expire / not cap: %+v", p.Free)
			}
		}
	}
	if !found {
		t.Fatal("charlespikachu-free provider not found")
	}
	_, ok := cfg.Group("charlespikachu-free")
	if !ok {
		t.Fatal("charlespikachu-free group not found")
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
