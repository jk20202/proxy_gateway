package alert

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"proxy-pool/internal/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWebhookDelivered(t *testing.T) {
	var hits atomic.Int32
	var gotType atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		b, _ := io.ReadAll(r.Body)
		gotType.Store(string(b))
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(config.AlertConfig{
		Webhooks:     []config.WebhookConfig{{URL: srv.URL, Events: []string{"provider_down"}}},
		DedupSeconds: 0,
	}, discardLogger())
	d.Emit(Event{Type: EventProviderDown, Provider: "p1", Message: "down"})
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("expected 1 webhook hit, got %d", hits.Load())
	}
}

func TestWebhookSecretHeader(t *testing.T) {
	var hits atomic.Int32
	var gotHeader atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotHeader.Store(r.Header.Get("X-Webhook-Secret"))
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(config.AlertConfig{
		Webhooks:     []config.WebhookConfig{{URL: srv.URL, Events: []string{"provider_down"}, Secret: "sec-42"}},
		DedupSeconds: 0,
	}, discardLogger())
	d.Emit(Event{Type: EventProviderDown, Provider: "p1", Message: "down"})
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("expected 1 webhook hit, got %d", hits.Load())
	}
	if h, _ := gotHeader.Load().(string); h != "sec-42" {
		t.Fatalf("expected X-Webhook-Secret sec-42, got %q", h)
	}
}

func TestWebhookFilterByEvent(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	d := NewDispatcher(config.AlertConfig{
		Webhooks:     []config.WebhookConfig{{URL: srv.URL, Events: []string{"provider_down"}}},
		DedupSeconds: 0,
	}, discardLogger())
	d.Emit(Event{Type: EventProviderRecovered, Provider: "p1", Message: "rec"})
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 0 {
		t.Fatalf("expected 0 webhook hits (event filtered), got %d", hits.Load())
	}
}

func TestDedupSuppressesRepeats(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(config.AlertConfig{
		Webhooks:     []config.WebhookConfig{{URL: srv.URL}},
		DedupSeconds: 60,
	}, discardLogger())
	d.Emit(Event{Type: EventProviderDown, Provider: "p1"})
	d.Emit(Event{Type: EventProviderDown, Provider: "p1"})
	d.Emit(Event{Type: EventProviderDown, Provider: "p1"})
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("expected 1 webhook hit after dedup, got %d", hits.Load())
	}
}

func TestDedupAllowsRecovery(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(config.AlertConfig{
		Webhooks:     []config.WebhookConfig{{URL: srv.URL}},
		DedupSeconds: 60,
	}, discardLogger())
	d.Emit(Event{Type: EventProviderDown, Provider: "p1"})
	d.Emit(Event{Type: EventProviderRecovered, Provider: "p1"})
	d.Emit(Event{Type: EventProviderDown, Provider: "p1"})
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 3 {
		t.Fatalf("expected 3 hits (down/recovered/down distinct keys), got %d", hits.Load())
	}
}

func TestAddRemoveWebhook(t *testing.T) {
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(filepath.Join(t.TempDir(), "alerts.json"))
	if err := d.AddWebhook(config.WebhookConfig{URL: "https://a.example.com/hook", Events: []string{"provider_down"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AddWebhook(config.WebhookConfig{URL: "https://a.example.com/hook", Events: nil}); err == nil {
		t.Fatal("expected duplicate url to be rejected")
	}
	if err := d.AddWebhook(config.WebhookConfig{URL: "  "}); err == nil {
		t.Fatal("expected empty url to be rejected")
	}
	if err := d.RemoveWebhook("https://a.example.com/hook"); err != nil {
		t.Fatal(err)
	}
	if err := d.RemoveWebhook("https://a.example.com/hook"); err == nil {
		t.Fatal("expected remove of missing webhook to fail")
	}
}

func TestUpdateEmailKeepsPassword(t *testing.T) {
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(filepath.Join(t.TempDir(), "alerts.json"))
	if err := d.UpdateEmail(config.EmailConfig{SMTPHost: "smtp.x.com", From: "a@x.com", To: []string{"b@x.com"}, Password: "secret"}); err != nil {
		t.Fatal(err)
	}
	// empty password preserves old one
	if err := d.UpdateEmail(config.EmailConfig{SMTPHost: "smtp.y.com", From: "a@y.com", To: []string{"b@y.com"}}); err != nil {
		t.Fatal(err)
	}
	cfg := d.GetConfig()
	if cfg.Email.SMTPHost != "smtp.y.com" {
		t.Fatalf("smtp host not updated: %+v", cfg.Email)
	}
	if cfg.Email.Password != "secret" {
		t.Fatalf("password not preserved, got %q", cfg.Email.Password)
	}
}

func TestUpdateEmailKeepsPort(t *testing.T) {
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(filepath.Join(t.TempDir(), "alerts.json"))
	if err := d.UpdateEmail(config.EmailConfig{SMTPHost: "smtp.x.com", SMTPPort: 465, From: "a@x.com", To: []string{"b@x.com"}, Password: "secret"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateEmail(config.EmailConfig{SMTPHost: "smtp.y.com", From: "a@y.com", To: []string{"b@y.com"}}); err != nil {
		t.Fatal(err)
	}
	cfg := d.GetConfig()
	if cfg.Email.SMTPPort != 465 {
		t.Fatalf("port not preserved, got %d", cfg.Email.SMTPPort)
	}
}

func TestConfigPersistsAcrossReload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "alerts.json")
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(file)
	if err := d.AddWebhook(config.WebhookConfig{URL: "https://persist.example.com/hook", Events: []string{"provider_down", "pool_exhausted"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateDedup(120); err != nil {
		t.Fatal(err)
	}

	d2 := NewDispatcher(config.AlertConfig{}, discardLogger())
	d2.SetFile(file)
	if err := d2.LoadFile(); err != nil {
		t.Fatal(err)
	}
	cfg := d2.GetConfig()
	if len(cfg.Webhooks) != 1 || cfg.Webhooks[0].URL != "https://persist.example.com/hook" {
		t.Fatalf("webhook not restored: %+v", cfg.Webhooks)
	}
	if cfg.DedupSeconds != 120 {
		t.Fatalf("dedup not restored: %d", cfg.DedupSeconds)
	}
}

func TestLoadFileMissingIsNoop(t *testing.T) {
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err := d.LoadFile(); err != nil {
		t.Fatal(err)
	}
	if cfg := d.GetConfig(); cfg.DedupSeconds != 60 {
		t.Fatalf("cfg should be untouched, got %d", cfg.DedupSeconds)
	}
}

func TestLoadFileCorruptReturnsError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "alerts.json")
	_ = os.WriteFile(file, []byte("{not-json"), 0o600)
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(file)
	if err := d.LoadFile(); err == nil {
		t.Fatal("expected error on corrupt file")
	}
}

func TestUpdateRecoverSeconds(t *testing.T) {
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(filepath.Join(t.TempDir(), "alerts.json"))
	if err := d.UpdateRecoverSeconds(1800); err != nil {
		t.Fatal(err)
	}
	if cfg := d.GetConfig(); cfg.RecoverSeconds != 1800 {
		t.Fatalf("recover not updated: %d", cfg.RecoverSeconds)
	}
	// negative rejected
	if err := d.UpdateRecoverSeconds(-1); err == nil {
		t.Fatal("expected error for negative recover interval")
	}
	// zero allowed (disables self-heal)
	if err := d.UpdateRecoverSeconds(0); err != nil {
		t.Fatal(err)
	}
	if cfg := d.GetConfig(); cfg.RecoverSeconds != 0 {
		t.Fatalf("recover should be 0, got %d", cfg.RecoverSeconds)
	}
}

func TestRecoverPersistsAcrossReload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "alerts.json")
	d := NewDispatcher(config.AlertConfig{DedupSeconds: 60}, discardLogger())
	d.SetFile(file)
	if err := d.UpdateRecoverSeconds(7200); err != nil {
		t.Fatal(err)
	}
	d2 := NewDispatcher(config.AlertConfig{}, discardLogger())
	d2.SetFile(file)
	if err := d2.LoadFile(); err != nil {
		t.Fatal(err)
	}
	if cfg := d2.GetConfig(); cfg.RecoverSeconds != 7200 {
		t.Fatalf("recover not restored: %d", cfg.RecoverSeconds)
	}
}
