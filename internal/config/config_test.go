package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := `
server:
  listen: ":9000"
providers:
  - name: t1
    type: tunnel
    enabled: true
    tunnel:
      gateway: gw.example.com
      port: 8080
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen != ":9000" {
		t.Fatalf("listen=%q", cfg.Server.Listen)
	}
	if cfg.Server.MaxWorkers == 0 {
		t.Fatal("max_workers default not applied")
	}
	if cfg.HealthCheck.MaxFails == 0 {
		t.Fatal("max_fails default not applied")
	}
	if cfg.Providers[0].Weight == 0 {
		t.Fatal("weight default not applied")
	}
}

func TestValidateRejectsBadType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := `
providers:
  - name: bad
    type: unknown
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestValidateTunnelRequiresConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := `
providers:
  - name: bad
    type: tunnel
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing tunnel config")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/path.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}
}
