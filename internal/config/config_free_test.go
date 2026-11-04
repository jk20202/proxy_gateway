package config

import (
	"os"
	"path/filepath"
	"testing"
)

// freeConfigFixture is a minimal YAML that exercises free-provider + free-group
// parsing independently of the deployment config.yaml.
const freeConfigFixture = `
health_check:
  interval_s: 60
  timeout_ms: 6000
  concurrency: 256
  check_urls:
    - { name: "ipapi-cc", url: "http://ip-api.com/line/?fields=countryCode", enabled: true }
    - { name: "ipapi-json", url: "http://ip-api.com/json/?fields=countryCode,query", enabled: true }
    - { name: "ipify", url: "https://api.ipify.org", enabled: true }
    - { name: "ipsb", url: "https://ip.sb", enabled: true }
    - { name: "icanhazip", url: "https://icanhazip.com", enabled: true }
providers:
  - name: "charlespikachu-free"
    type: "free"
    enabled: true
    min_alive_ratio: 0
    check_url: "http://www.gstatic.com/generate_204"
    free:
      feed_url: "https://charlespikachu.github.io/freeproxy/proxies.json"
      refresh_interval_s: 600
      max_speed_ms: 3000
      delete_latency_ms: 3000
groups:
  - name: "charlespikachu-free"
    type: "free"
    min_alive_ratio: 0
    primary: ["charlespikachu-free"]
`

func TestLoadFreeConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(freeConfigFixture), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.HealthCheck.CheckURLs) != 5 {
		t.Fatalf("fixture should define 5 default check_urls, got %d", len(cfg.HealthCheck.CheckURLs))
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
	if _, ok := cfg.Group("charlespikachu-free"); !ok {
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
