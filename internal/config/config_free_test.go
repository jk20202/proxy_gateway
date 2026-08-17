package config

import "testing"

func TestLoadFreeConfig(t *testing.T) {
	cfg, err := Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
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
