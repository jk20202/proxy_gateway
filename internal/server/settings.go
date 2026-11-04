package server

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"proxy-pool/internal/config"
)

// SettingsStore holds runtime-adjustable system settings (site name, whether
// registration is open, default theme, and health-check tuning) and persists
// them to a JSON file so changes survive restarts. The onApply hook is invoked
// after every update so hot-applicable settings (health-check params) can be
// pushed into the running checker without a restart.
type SettingsStore struct {
	mu       sync.RWMutex
	path     string
	settings config.Settings
	onApply  func(config.Settings)
}

// NewSettingsStore loads settings from path, falling back to def for any field
// not present in the file. An empty path disables persistence (in-memory only).
func NewSettingsStore(path string, def config.Settings) (*SettingsStore, error) {
	s := &SettingsStore{path: path, settings: def}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SettingsStore) load() error {
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var cfg config.Settings
	if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}
	def := s.settings
	if cfg.SiteName == "" {
		cfg.SiteName = def.SiteName
	}
	if cfg.ThemeDefault == "" {
		cfg.ThemeDefault = def.ThemeDefault
	}
	if cfg.HealthCheck.IntervalSeconds <= 0 {
		cfg.HealthCheck.IntervalSeconds = def.HealthCheck.IntervalSeconds
	}
	if cfg.HealthCheck.TimeoutMs <= 0 {
		cfg.HealthCheck.TimeoutMs = def.HealthCheck.TimeoutMs
	}
	if cfg.HealthCheck.Concurrency <= 0 {
		cfg.HealthCheck.Concurrency = def.HealthCheck.Concurrency
	}
	if cfg.HealthCheck.MaxFails <= 0 {
		cfg.HealthCheck.MaxFails = def.HealthCheck.MaxFails
	}
	if len(cfg.HealthCheck.CheckURLs) == 0 {
		cfg.HealthCheck.CheckURLs = def.HealthCheck.CheckURLs
	}
	s.settings = cfg
	return nil
}

// Get returns a snapshot of the current settings.
func (s *SettingsStore) Get() config.Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

// SetOnApply registers the hook invoked after every successful update.
func (s *SettingsStore) SetOnApply(fn func(config.Settings)) {
	s.mu.Lock()
	s.onApply = fn
	s.mu.Unlock()
}

// Update validates, persists and applies new settings.
func (s *SettingsStore) Update(cfg config.Settings) error {
	if cfg.SiteName == "" {
		return errors.New("site_name is required")
	}
	if cfg.ThemeDefault != "" && cfg.ThemeDefault != "dark" && cfg.ThemeDefault != "light" {
		return errors.New("theme_default must be dark or light")
	}
	if cfg.HealthCheck.IntervalSeconds <= 0 {
		cfg.HealthCheck.IntervalSeconds = 60
	}
	if cfg.HealthCheck.TimeoutMs <= 0 {
		cfg.HealthCheck.TimeoutMs = 5000
	}
	if cfg.HealthCheck.Concurrency <= 0 {
		cfg.HealthCheck.Concurrency = 64
	}
	if cfg.HealthCheck.MaxFails <= 0 {
		cfg.HealthCheck.MaxFails = 3
	}
	// Preserve the legacy single check_url and the rebuild interval, which are
	// not exposed in the web console but are still honored by the checker.
	{
		cur := s.Get()
		if cfg.HealthCheck.CheckURL == "" {
			cfg.HealthCheck.CheckURL = cur.HealthCheck.CheckURL
		}
		if cfg.HealthCheck.RebuildIntervalMs == 0 {
			cfg.HealthCheck.RebuildIntervalMs = cur.HealthCheck.RebuildIntervalMs
		}
	}
	s.mu.Lock()
	s.settings = cfg
	s.mu.Unlock()
	if err := s.persist(); err != nil {
		return err
	}
	if s.onApply != nil {
		s.onApply(cfg)
	}
	return nil
}

func (s *SettingsStore) persist() error {
	if s.path == "" {
		return nil
	}
	data, err := json.MarshalIndent(s.settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}
