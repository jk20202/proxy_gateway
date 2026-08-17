package alert

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/pool"
)

type mockMonitorMgr struct {
	refreshCalls atomic.Int32
	info         pool.ProviderInfo
}

func (m *mockMonitorMgr) ProviderList() []pool.ProviderInfo {
	return []pool.ProviderInfo{m.info}
}

func (m *mockMonitorMgr) RefreshProvider(ctx context.Context, name string) error {
	m.refreshCalls.Add(1)
	return nil
}

func (m *mockMonitorMgr) Providers() map[string]any { return nil }

func newMockMonitor(pi pool.ProviderInfo, recoverSec int) (*ProviderMonitor, *mockMonitorMgr, *Dispatcher) {
	m := &mockMonitorMgr{info: pi}
	dis := NewDispatcher(config.AlertConfig{RecoverSeconds: recoverSec}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mon := newProviderMonitorWithMgr(m, config.AlertConfig{RecoverSeconds: recoverSec}, dis, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mon, m, dis
}

func TestRecoverySelfCheckRefreshesDownProvider(t *testing.T) {
	pi := pool.ProviderInfo{Name: "p", Total: 5, Alive: 0, MinAliveRatio: 0.5, Enabled: true}
	mon, m, _ := newMockMonitor(pi, 1)
	mon.check()
	mon.mu.Lock()
	mon.downSince["p"] = time.Now().Add(-2 * time.Second)
	mon.mu.Unlock()
	mon.check()
	if m.refreshCalls.Load() < 1 {
		t.Fatal("expected at least one recovery refresh for down provider")
	}
}

func TestRecoveryThrottledByInterval(t *testing.T) {
	pi := pool.ProviderInfo{Name: "p", Total: 5, Alive: 0, MinAliveRatio: 0.5, Enabled: true}
	mon, m, _ := newMockMonitor(pi, 3600)
	mon.check()
	mon.check()
	mon.check()
	if calls := m.refreshCalls.Load(); calls > 1 {
		t.Fatalf("recovery refresh should be throttled, got %d calls", calls)
	}
}

func TestNoRecoveryWhenRecoverDisabled(t *testing.T) {
	pi := pool.ProviderInfo{Name: "p", Total: 5, Alive: 0, MinAliveRatio: 0.5, Enabled: true}
	mon, m, _ := newMockMonitor(pi, 0)
	mon.check()
	mon.check()
	if m.refreshCalls.Load() != 0 {
		t.Fatal("no recovery refresh expected when recover_interval_s=0")
	}
}

func TestNoRecoveryWhenProviderHealthy(t *testing.T) {
	pi := pool.ProviderInfo{Name: "p", Total: 5, Alive: 5, MinAliveRatio: 0.5, Enabled: true}
	mon, m, _ := newMockMonitor(pi, 1)
	mon.check()
	mon.check()
	if m.refreshCalls.Load() != 0 {
		t.Fatal("no recovery refresh expected for healthy provider")
	}
}

func TestRecoveryRefreshesAgainAfterInterval(t *testing.T) {
	pi := pool.ProviderInfo{Name: "p", Total: 5, Alive: 0, MinAliveRatio: 0.5, Enabled: true}
	mon, m, _ := newMockMonitor(pi, 1)
	mon.check()
	// simulate two elapsed recover intervals
	for range 2 {
		mon.mu.Lock()
		mon.downSince["p"] = time.Now().Add(-2 * time.Second)
		mon.lastRecoverAttempt["p"] = time.Time{}
		mon.mu.Unlock()
		mon.check()
	}
	if calls := m.refreshCalls.Load(); calls != 2 {
		t.Fatalf("expected two recovery refreshes, got %d", calls)
	}
}

func TestIsProviderDown(t *testing.T) {
	if !isProviderDown(pool.ProviderInfo{Total: 0}) {
		t.Fatal("zero total should be down")
	}
	if !isProviderDown(pool.ProviderInfo{Total: 10, Alive: 0, MinAliveRatio: 0.5}) {
		t.Fatal("0/10 with 0.5 ratio should be down")
	}
	if isProviderDown(pool.ProviderInfo{Total: 10, Alive: 6, MinAliveRatio: 0.5}) {
		t.Fatal("6/10 with 0.5 ratio should be up")
	}
	if isProviderDown(pool.ProviderInfo{Total: 10, Alive: 1}) {
		t.Fatal("1/10 with no ratio should be up")
	}
	if !isProviderDown(pool.ProviderInfo{Total: 10, Alive: 0}) {
		t.Fatal("0/10 with no ratio should be down")
	}
}
