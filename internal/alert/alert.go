package alert

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
	"sync"
	"time"

	"proxy-pool/internal/config"
)

// Event types
const (
	EventProviderDown      = "provider_down"
	EventProviderRecovered = "provider_recovered"
	EventPoolExhausted     = "pool_exhausted"
	EventRefreshFailed     = "refresh_failed"
	EventProviderRemoved   = "provider_removed"
)

type Event struct {
	Type      string         `json:"type"`
	Provider  string         `json:"provider,omitempty"`
	Message   string         `json:"message"`
	Data      map[string]any `json:"data,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

type Dispatcher struct {
	cfg      config.AlertConfig
	logger   *slog.Logger
	hc       *http.Client
	mu       sync.Mutex
	dedup    map[string]time.Time
	filePath string // optional: persist config here on mutation
}

func NewDispatcher(cfg config.AlertConfig, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		cfg:    cfg,
		logger: logger,
		hc:     &http.Client{Timeout: 10 * time.Second},
		dedup:  make(map[string]time.Time),
	}
}

// SetFile enables JSON persistence: config is saved here after each mutation.
func (d *Dispatcher) SetFile(path string) {
	d.filePath = path
}

// LoadFile reads persisted config if present, overriding the initial one.
func (d *Dispatcher) LoadFile() error {
	if d.filePath == "" {
		return nil
	}
	data, err := os.ReadFile(d.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	cfg := config.AlertConfig{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}
	d.mu.Lock()
	d.cfg = cfg
	d.mu.Unlock()
	return nil
}

// GetConfig returns a copy of the current alert configuration.
func (d *Dispatcher) GetConfig() config.AlertConfig {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cfg
}

// AddWebhook appends a webhook and persists the config.
func (d *Dispatcher) AddWebhook(wh config.WebhookConfig) error {
	if strings.TrimSpace(wh.URL) == "" {
		return errors.New("webhook url required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, w := range d.cfg.Webhooks {
		if w.URL == wh.URL {
			return errors.New("webhook already exists")
		}
	}
	d.cfg.Webhooks = append(d.cfg.Webhooks, wh)
	return d.persist()
}

// RemoveWebhook removes a webhook by URL and persists the config.
func (d *Dispatcher) RemoveWebhook(url string) error {
	if url == "" {
		return errors.New("webhook url required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, w := range d.cfg.Webhooks {
		if w.URL == url {
			d.cfg.Webhooks = append(d.cfg.Webhooks[:i], d.cfg.Webhooks[i+1:]...)
			return d.persist()
		}
	}
	return errors.New("webhook not found")
}

// UpdateWebhook replaces the webhook matching oldURL and persists the config.
func (d *Dispatcher) UpdateWebhook(oldURL string, wh config.WebhookConfig) error {
	if strings.TrimSpace(wh.URL) == "" {
		return errors.New("webhook url required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, w := range d.cfg.Webhooks {
		if w.URL == oldURL {
			d.cfg.Webhooks[i] = wh
			return d.persist()
		}
	}
	return errors.New("webhook not found")
}

// UpdateEmail replaces the email settings and persists the config.
// An empty Password or zero SMTPPort preserves the existing value.
func (d *Dispatcher) UpdateEmail(e config.EmailConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e.Password == "" {
		e.Password = d.cfg.Email.Password
	}
	if e.SMTPPort == 0 {
		e.SMTPPort = d.cfg.Email.SMTPPort
	}
	d.cfg.Email = e
	return d.persist()
}

// UpdateDedup sets the dedup window in seconds and persists the config.
func (d *Dispatcher) UpdateDedup(seconds int) error {
	if seconds < 0 {
		return errors.New("dedup_seconds must be >= 0")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.DedupSeconds = seconds
	return d.persist()
}

// UpdateMonitorSeconds sets the provider monitor interval in seconds and persists it.
func (d *Dispatcher) UpdateMonitorSeconds(seconds int) error {
	if seconds < 1 {
		return errors.New("monitor interval must be >= 1 second")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.MonitorSeconds = seconds
	return d.persist()
}

// UpdateRecoverSeconds sets the provider recovery self-check interval in
// seconds and persists it.
func (d *Dispatcher) UpdateRecoverSeconds(seconds int) error {
	if seconds < 0 {
		return errors.New("recover interval must be >= 0")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.RecoverSeconds = seconds
	return d.persist()
}

// persist writes the current config to disk. Caller must hold d.mu.
func (d *Dispatcher) persist() error {
	if d.filePath == "" {
		return nil
	}
	data, err := json.MarshalIndent(d.cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(d.filePath, data, 0o600)
}

// Emit sends an alert through all configured channels, honoring per-key
// dedup so repeated identical events are only delivered once per window.
func (d *Dispatcher) Emit(ev Event) {
	d.mu.Lock()
	cfg := d.cfg
	if len(cfg.Webhooks) == 0 && !emailEnabled(cfg.Email) {
		d.mu.Unlock()
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}

	key := ev.Type + ":" + ev.Provider
	// opposite event clears dedup so a new incident of this type can fire again
	switch ev.Type {
	case EventProviderDown:
		delete(d.dedup, EventProviderRecovered+":"+ev.Provider)
	case EventProviderRecovered:
		delete(d.dedup, EventProviderDown+":"+ev.Provider)
	}
	window := time.Duration(cfg.DedupSeconds) * time.Second
	if last, ok := d.dedup[key]; ok && time.Since(last) < window {
		d.mu.Unlock()
		return
	}
	d.dedup[key] = time.Now()
	// prune old entries occasionally
	if len(d.dedup) > 128 {
		for k, t := range d.dedup {
			if time.Since(t) >= window {
				delete(d.dedup, k)
			}
		}
	}
	d.mu.Unlock()

	if emailEnabled(cfg.Email) {
		go d.sendEmail(cfg.Email, ev)
	}
	for _, wh := range cfg.Webhooks {
		if !eventMatch(wh.Events, ev.Type) {
			continue
		}
		go d.sendWebhook(wh, ev)
	}
}

func emailEnabled(e config.EmailConfig) bool {
	return e.SMTPHost != "" && e.From != "" && len(e.To) > 0
}

func eventMatch(patterns []string, typ string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if p == typ || p == "*" || p == "all" {
			return true
		}
	}
	return false
}

func (d *Dispatcher) sendWebhook(wh config.WebhookConfig, ev Event) {
	body, err := json.Marshal(ev)
	if err != nil {
		d.logger.Warn("alert marshal failed", "err", err)
		return
	}
	req, err := http.NewRequest(http.MethodPost, wh.URL, bytes.NewReader(body))
	if err != nil {
		d.logger.Warn("alert webhook request failed", "url", wh.URL, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if wh.Secret != "" {
		req.Header.Set("X-Webhook-Secret", wh.Secret)
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		d.logger.Warn("alert webhook send failed", "url", wh.URL, "event", ev.Type, "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		d.logger.Warn("alert webhook non-2xx", "url", wh.URL, "status", resp.StatusCode)
	}
}

func (d *Dispatcher) sendEmail(e config.EmailConfig, ev Event) {
	subject := fmt.Sprintf("[ProxyPool] %s %s", ev.Type, ev.Provider)
	body := fmt.Sprintf("时间: %s\n事件: %s\nProvider: %s\n\n%s\n",
		ev.Timestamp.Format("2006-01-02 15:04:05"), ev.Type, ev.Provider, ev.Message)
	if len(ev.Data) > 0 {
		body += "\n详情:\n"
		for k, v := range ev.Data {
			body += fmt.Sprintf("  %s: %v\n", k, v)
		}
	}

	addr := fmt.Sprintf("%s:%d", e.SMTPHost, e.SMTPPort)
	from := e.From
	if from == "" {
		from = e.Username
	}

	var client *smtp.Client
	var conn net.Conn
	var err error
	if e.UseTLS {
		conn, err = tls.Dial("tcp", addr, &tls.Config{ServerName: e.SMTPHost})
		if err == nil {
			client, err = smtp.NewClient(conn, e.SMTPHost)
		}
	} else {
		client, err = smtp.Dial(addr)
	}
	if err != nil {
		d.logger.Warn("alert email connect failed", "host", e.SMTPHost, "err", err)
		return
	}
	defer func() {
		_ = client.Quit()
		if conn != nil {
			_ = conn.Close()
		}
	}()

	if !e.UseTLS {
		if err = client.StartTLS(&tls.Config{ServerName: e.SMTPHost}); err != nil {
			d.logger.Warn("alert email starttls failed", "host", e.SMTPHost, "err", err)
			return
		}
	}
	if e.Username != "" {
		auth := smtp.PlainAuth("", e.Username, e.Password, e.SMTPHost)
		if err = client.Auth(auth); err != nil {
			d.logger.Warn("alert email auth failed", "err", err)
			return
		}
	}
	if err = client.Mail(from); err != nil {
		d.logger.Warn("alert email mail-from failed", "err", err)
		return
	}
	for _, to := range e.To {
		if _, err := mail.ParseAddress(to); err != nil {
			continue
		}
		if err := client.Rcpt(to); err != nil {
			d.logger.Warn("alert email rcpt failed", "to", to, "err", err)
		}
	}
	w, err := client.Data()
	if err != nil {
		d.logger.Warn("alert email data failed", "err", err)
		return
	}
	msg := "From: " + from + "\r\n" +
		"To: " + strings.Join(e.To, ", ") + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n" +
		body
	_, _ = w.Write([]byte(msg))
	_ = w.Close()
}
