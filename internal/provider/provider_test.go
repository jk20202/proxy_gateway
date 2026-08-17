package provider

import (
	"context"
	"testing"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

func TestTunnelInitial(t *testing.T) {
	cfg := config.ProviderCfg{
		Name:   "t1",
		Type:   "tunnel",
		Weight: 100,
		Tunnel: &config.TunnelConfig{
			Scheme:   "http",
			Gateway:  "gw.example.com",
			Port:     8080,
			Username: "u",
			Password: "p",
		},
	}
	prov, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if prov.Kind() != model.KindTunnel {
		t.Fatalf("expected tunnel kind")
	}
	proxies, err := prov.Initial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(proxies) != 1 {
		t.Fatalf("expected 1 proxy, got %d", len(proxies))
	}
	pr := proxies[0]
	if pr.Addr() != "gw.example.com:8080" {
		t.Fatalf("unexpected addr %s", pr.Addr())
	}
	if pr.Username != "u" || pr.Password != "p" {
		t.Fatal("auth not propagated")
	}
	if !pr.Alive.Load() {
		t.Fatal("proxy should be alive")
	}
}

func TestSplitAddr(t *testing.T) {
	cases := []struct {
		in   string
		host string
		port string
		ok   bool
	}{
		{"1.2.3.4:8080", "1.2.3.4", "8080", true},
		{"http://1.2.3.4:8080", "1.2.3.4", "8080", true},
		{"1.2.3.4", "", "", false},
		{"", "", "", false},
	}
	for _, c := range cases {
		h, p, ok := splitAddr(c.in)
		if ok != c.ok || h != c.host || p != c.port {
			t.Fatalf("splitAddr(%q) = %q,%q,%v want %q,%q,%v", c.in, h, p, ok, c.host, c.port, c.ok)
		}
	}
}
