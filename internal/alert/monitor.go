package alert

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/pool"
)

// ManagerProvider is the subset of *pool.Manager that the monitor needs,
// allowing easy testing.
type ManagerProvider interface {
	ProviderList() []pool.ProviderInfo
	RefreshProvider(ctx context.Context, name string) error
}

// ProviderMonitor periodically inspects each provider's alive ratio and
// emits provider_down / provider_recovered events on state transitions. It
// also force-refreshes down providers after recover_interval_s to attempt
// automatic recovery.
type ProviderMonitor struct {
	mgr    ManagerProvider
	cfg    config.AlertConfig
	logger *slog.Logger
	dis    *Dispatcher

	mu                 sync.Mutex
	states             map[string]bool // provider -> down
	downSince          map[string]time.Time
	lastRecoverAttempt map[string]time.Time
}

func NewProviderMonitor(mgr *pool.Manager, cfg config.AlertConfig, dis *Dispatcher, logger *slog.Logger) *ProviderMonitor {
	return &ProviderMonitor{
		mgr:                mgr,
		cfg:                cfg,
		logger:             logger,
		dis:                dis,
		states:             make(map[string]bool),
		downSince:          make(map[string]time.Time),
		lastRecoverAttempt: make(map[string]time.Time),
	}
}

// newProviderMonitorWithMgr allows tests to inject a fake manager.
func newProviderMonitorWithMgr(mgr ManagerProvider, cfg config.AlertConfig, dis *Dispatcher, logger *slog.Logger) *ProviderMonitor {
	return &ProviderMonitor{
		mgr:                mgr,
		cfg:                cfg,
		logger:             logger,
		dis:                dis,
		states:             make(map[string]bool),
		downSince:          make(map[string]time.Time),
		lastRecoverAttempt: make(map[string]time.Time),
	}
}

func (m *ProviderMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	m.check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			interval := m.effectiveInterval()
			ticker.Reset(interval)
			m.check()
		}
	}
}

// effectiveInterval returns the monitor interval from the dispatcher config,
// allowing runtime changes to take effect on the next tick.
func (m *ProviderMonitor) effectiveInterval() time.Duration {
	cfg := m.dis.GetConfig()
	if cfg.MonitorSeconds > 0 {
		return time.Duration(cfg.MonitorSeconds) * time.Second
	}
	return 15 * time.Second
}

func (m *ProviderMonitor) CheckOnce() {
	m.check()
}

func (m *ProviderMonitor) check() {
	for _, pi := range m.mgr.ProviderList() {
		if !pi.Enabled {
			m.mu.Lock()
			if wasDown, ok := m.states[pi.Name]; ok && wasDown {
				m.states[pi.Name] = false
				m.dis.Emit(Event{
					Type:     EventProviderRecovered,
					Provider: pi.Name,
					Message:  "provider " + pi.Name + " re-enabled by admin",
				})
			} else {
				m.states[pi.Name] = false
			}
			m.mu.Unlock()
			continue
		}

		down := isProviderDown(pi)
		m.mu.Lock()
		wasDown := m.states[pi.Name]
		m.states[pi.Name] = down
		if down && !wasDown {
			m.downSince[pi.Name] = time.Now()
		}
		if !down {
			delete(m.downSince, pi.Name)
			delete(m.lastRecoverAttempt, pi.Name)
		}
		m.mu.Unlock()

		if down {
			m.maybeAttemptRecovery(pi.Name)
		}

		// 免费代理池不加告警：free 代理检测失败/延迟超标会被直接删除，
		// 池内代理频繁进出，告警没有意义。手动添加的（tunnel/ip_pool/sticky）才有告警。
		if pi.ProviderType == "free" {
			continue
		}

		if down && !wasDown {
			m.dis.Emit(Event{
				Type:     EventProviderDown,
				Provider: pi.Name,
				Message: "provider " + pi.Name + " is down (alive " +
					strconv.Itoa(pi.Alive) + "/" + strconv.Itoa(pi.Total) + ")",
				Data: map[string]any{
					"alive":           pi.Alive,
					"total":           pi.Total,
					"min_alive_ratio": pi.MinAliveRatio,
					"priority":        pi.Priority,
				},
			})
			m.logger.Warn("alert: provider down", "provider", pi.Name, "alive", pi.Alive, "total", pi.Total)
		} else if !down && wasDown {
			m.dis.Emit(Event{
				Type:     EventProviderRecovered,
				Provider: pi.Name,
				Message: "provider " + pi.Name + " recovered (alive " +
					strconv.Itoa(pi.Alive) + "/" + strconv.Itoa(pi.Total) + ")",
				Data: map[string]any{
					"alive": pi.Alive,
					"total": pi.Total,
				},
			})
			m.logger.Info("alert: provider recovered", "provider", pi.Name, "alive", pi.Alive, "total", pi.Total)
		}
	}
}

func isProviderDown(pi pool.ProviderInfo) bool {
	if pi.Total == 0 {
		return true
	}
	ratio := float64(pi.Alive) / float64(pi.Total)
	if pi.MinAliveRatio > 0 {
		return ratio < pi.MinAliveRatio
	}
	return pi.Alive == 0
}

// maybeAttemptRecovery force-refreshes a down provider once recover_interval
// has elapsed since it went down, then throttles further attempts to the same
// interval. This lets the pool self-heal without admin intervention.
func (m *ProviderMonitor) maybeAttemptRecovery(name string) {
	interval := m.recoverInterval()
	if interval <= 0 {
		return
	}
	m.mu.Lock()
	since := m.downSince[name]
	last := m.lastRecoverAttempt[name]
	now := time.Now()
	if since.IsZero() || now.Sub(since) < interval {
		m.mu.Unlock()
		return
	}
	if !last.IsZero() && now.Sub(last) < interval {
		m.mu.Unlock()
		return
	}
	m.lastRecoverAttempt[name] = now
	m.mu.Unlock()

	m.logger.Info("alert: attempting provider recovery", "provider", name)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m.mgr.RefreshProvider(ctx, name); err != nil {
		m.logger.Warn("alert: provider recovery refresh failed", "provider", name, "err", err)
	}
}

func (m *ProviderMonitor) recoverInterval() time.Duration {
	cfg := m.dis.GetConfig()
	if cfg.RecoverSeconds > 0 {
		return time.Duration(cfg.RecoverSeconds) * time.Second
	}
	if m.cfg.RecoverSeconds > 0 {
		return time.Duration(m.cfg.RecoverSeconds) * time.Second
	}
	return 0
}
