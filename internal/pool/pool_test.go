package pool

import (
	"sync"
	"testing"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

func newTestProxy(id string, weight int32) *model.Proxy {
	pr := &model.Proxy{
		ID:       id,
		Provider: "test",
		Kind:     model.KindTunnel,
		Scheme:   "http",
		Host:     "127.0.0.1",
		Port:     8000,
		Weight:   weight,
	}
	pr.Alive.Store(true)
	return pr
}

func newTestProxyPrio(id string, weight int32, priority int, minRatio float64) *model.Proxy {
	pr := newTestProxy(id, weight)
	pr.Priority = priority
	pr.MinAliveRatio = minRatio
	return pr
}

func TestPriorityUsesHighestPrioGroup(t *testing.T) {
	p := NewPool()
	p.Add(newTestProxyPrio("backup1", 1, 0, 0))
	p.Add(newTestProxyPrio("backup2", 1, 0, 0))
	p.Add(newTestProxyPrio("primary", 1, 10, 0))

	for range 1000 {
		pr := p.Next()
		if pr == nil {
			t.Fatal("Next returned nil")
		}
		if pr.ID == "backup1" || pr.ID == "backup2" {
			t.Fatalf("backup proxy used while primary alive: %s", pr.ID)
		}
	}
}

func TestPriorityDowngradesWhenPrimaryAllDead(t *testing.T) {
	p := NewPool()
	backup := newTestProxyPrio("backup", 1, 0, 0)
	p.Add(backup)
	p.Add(newTestProxyPrio("primary", 1, 10, 0))

	// primary dies
	primary := p.Get("primary")
	if primary == nil {
		t.Fatal("primary not found")
	}
	p.SetAlive("primary", false)

	for range 100 {
		pr := p.Next()
		if pr == nil {
			t.Fatal("Next returned nil")
		}
		if pr.ID != "backup" {
			t.Fatalf("expected backup after primary dead, got %s", pr.ID)
		}
	}
}

func TestPriorityDowngradeByRatio(t *testing.T) {
	p := NewPool()
	// primary group: 10 proxies, min ratio 0.1 -> needs at least 1 alive
	for i := range 10 {
		p.Add(newTestProxyPrio("primary"+string(rune('a'+i)), 1, 10, 0.1))
	}
	p.Add(newTestProxyPrio("backup", 1, 0, 0))

	// kill primarya (i=0) and primaryb..primaryi (i=1..8), only primaryj alive
	p.SetAlive("primarya", false)
	for i := 1; i < 9; i++ {
		p.SetAlive("primary"+string(rune('a'+i)), false)
	}
	// alive: 1/10 = 0.1 >= 0.1 -> primary still used
	for range 100 {
		pr := p.Next()
		if pr == nil {
			t.Fatal("Next returned nil")
		}
		if pr.ID != "primaryj" {
			t.Fatalf("expected primaryj, got %s", pr.ID)
		}
	}

	// kill last primary -> 0/10 = 0 < 0.1 -> backup
	p.SetAlive("primaryj", false)
	for range 100 {
		pr := p.Next()
		if pr == nil {
			t.Fatal("Next returned nil")
		}
		if pr.ID != "backup" {
			t.Fatalf("expected backup, got %s", pr.ID)
		}
	}
}

func TestPriorityMinRatioRecovery(t *testing.T) {
	p := NewPool()
	backup := newTestProxyPrio("backup", 1, 0, 0)
	p.Add(backup)
	primary := newTestProxyPrio("primary", 1, 10, 0.1)
	p.Add(primary)

	// 1/1 = 1.0 alive -> primary used
	for range 100 {
		if pr := p.Next(); pr.ID != "primary" {
			t.Fatalf("expected primary, got %s", pr.ID)
		}
	}

	// primary dies -> backup
	p.SetAlive("primary", false)
	for range 100 {
		if pr := p.Next(); pr.ID != "backup" {
			t.Fatalf("expected backup, got %s", pr.ID)
		}
	}

	// primary recovers -> auto switch back
	p.SetAlive("primary", true)
	for range 100 {
		if pr := p.Next(); pr.ID != "primary" {
			t.Fatalf("expected primary after recovery, got %s", pr.ID)
		}
	}
}

func TestStickyReusesSameProxy(t *testing.T) {
	p := NewPool()
	for i := range 5 {
		p.Add(newTestProxy("sticky"+string(rune('a'+i)), 1))
	}
	first := p.StickyNext("client-1", 60, "")
	if first == nil {
		t.Fatal("StickyNext returned nil")
	}
	for range 100 {
		pr := p.StickyNext("client-1", 60, "")
		if pr == nil {
			t.Fatal("StickyNext returned nil")
		}
		if pr.ID != first.ID {
			t.Fatalf("sticky client changed proxy: %s -> %s", first.ID, pr.ID)
		}
	}
}

func TestStickyExpiresAfterWindow(t *testing.T) {
	p := NewPool()
	for i := range 3 {
		p.Add(newTestProxy("s"+string(rune('a'+i)), 1))
	}
	first := p.StickyNext("c", 1, "")
	if first == nil {
		t.Fatal("StickyNext returned nil")
	}
	// expired window: stickySeconds <= 0 means no stickiness
	pr := p.StickyNext("c", 0, "")
	if pr == nil {
		t.Fatal("StickyNext returned nil")
	}
	_ = pr
}

func TestStickyFallsBackWhenProxyDead(t *testing.T) {
	p := NewPool()
	p.Add(newTestProxy("sa", 1))
	p.Add(newTestProxy("sb", 1))
	first := p.StickyNext("c", 60, "")
	if first == nil {
		t.Fatal("StickyNext returned nil")
	}
	// kill sticky proxy -> next call re-picks a new one
	p.SetAlive(first.ID, false)
	got := p.StickyNext("c", 60, "")
	if got == nil {
		t.Fatal("StickyNext returned nil")
	}
	if got.ID == first.ID {
		t.Fatalf("expected a different proxy after sticky one died, got %s", got.ID)
	}
}

func TestAddAndNext(t *testing.T) {
	p := NewPool()
	for i := range 10 {
		p.Add(newTestProxy("p"+string(rune('a'+i)), 1))
	}
	seen := map[string]bool{}
	for range 100 {
		pr := p.Next()
		if pr == nil {
			t.Fatal("Next returned nil")
		}
		seen[pr.ID] = true
	}
	if len(seen) != 10 {
		t.Fatalf("expected 10 distinct proxies, got %d", len(seen))
	}
}

func TestWeightedDistribution(t *testing.T) {
	p := NewPool()
	p.Add(newTestProxy("heavy", 100))
	p.Add(newTestProxy("light", 1))

	heavyCount := 0
	total := 100000
	for range total {
		pr := p.Next()
		if pr.ID == "heavy" {
			heavyCount++
		}
	}
	ratio := float64(heavyCount) / float64(total)
	if ratio < 0.97 {
		t.Fatalf("heavy proxy ratio %f too low", ratio)
	}
}

func TestMarkFailedDeactivates(t *testing.T) {
	p := NewPool()
	pr := newTestProxy("a", 1)
	p.Add(pr)

	for range 3 {
		p.MarkFailed("a", 10)
	}
	if pr.Alive.Load() {
		t.Fatal("proxy should be dead after 3 fails")
	}
	if n := p.Next(); n != nil {
		t.Fatal("no proxy should be returned after all dead")
	}

	p.MarkSuccess("a", 5)
	if !pr.Alive.Load() {
		t.Fatal("proxy should be alive after success")
	}
	if n := p.Next(); n == nil {
		t.Fatal("proxy should be returned after recovery")
	}
}

func TestNextExcluding(t *testing.T) {
	p := NewPool()
	p.Add(newTestProxy("a", 1))
	p.Add(newTestProxy("b", 1))
	p.Add(newTestProxy("c", 1))

	exclude := map[string]struct{}{"a": {}, "b": {}}
	for range 50 {
		pr := p.NextExcluding(exclude)
		if pr == nil || pr.ID != "c" {
			t.Fatalf("expected c, got %v", pr)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	p := NewPool()
	for i := range 50 {
		p.Add(newTestProxy("p"+string(rune('a'+i)), 1))
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5000 {
				p.Next()
				p.MarkSuccess("pa", 1)
				p.MarkFailed("pb", 2)
			}
		}()
	}
	wg.Wait()

	_, alive := p.Stats()
	if alive != 49 {
		t.Fatalf("expected 49 alive, got %d", alive)
	}
}

func newGroupProxy(id, provider string, weight int32) *model.Proxy {
	pr := newTestProxy(id, weight)
	pr.Provider = provider
	return pr
}

func TestGroupFallbackToBackupTier(t *testing.T) {
	p := NewPool()
	for i := range 5 {
		p.Add(newGroupProxy("prim"+string(rune('a'+i)), "pA", 1))
	}
	for i := range 3 {
		p.Add(newGroupProxy("bkp"+string(rune('a'+i)), "pB", 1))
	}
	p.SetGroups([]config.GroupCfg{
		{Name: "g", MinAliveRatio: 0.5, Primary: []string{"pA"}, Backups: []config.BackupPool{{Name: "bk", MinAliveRatio: 0, Providers: []string{"pB"}}}},
	})

	for range 100 {
		if pr := p.NextInGroup("g"); pr == nil || pr.Provider != "pA" {
			t.Fatalf("expected primary tier while alive, got %v", pr)
		}
	}

	// kill 4/5 primary proxies -> ratio 0.2 < 0.5 -> backup tier
	for i := 0; i < 4; i++ {
		p.SetAlive("prim"+string(rune('a'+i)), false)
	}
	for range 100 {
		pr := p.NextInGroup("g")
		if pr == nil || pr.Provider != "pB" {
			t.Fatalf("expected backup tier after ratio drop, got %v", pr)
		}
	}

	// primary recovers -> back to primary tier
	p.SetAlive("prima", true)
	p.SetAlive("primb", true)
	for range 100 {
		if pr := p.NextInGroup("g"); pr == nil || pr.Provider != "pA" {
			t.Fatalf("expected primary tier after recovery, got %v", pr)
		}
	}
}

func TestGroupUnknownReturnsNil(t *testing.T) {
	p := NewPool()
	p.Add(newTestProxy("a", 1))
	p.SetGroups([]config.GroupCfg{{Name: "g", MinAliveRatio: 0, Primary: []string{"test"}}})
	if pr := p.NextInGroup("nope"); pr != nil {
		t.Fatalf("expected nil for unknown group, got %v", pr)
	}
}

func TestGroupSWRRWeights(t *testing.T) {
	p := NewPool()
	// single pA proxy weight 3, single pB proxy weight 1 -> 3:1, no tunnels -> SWRR
	p.Add(newGroupProxy("ha", "pA", 3))
	p.Add(newGroupProxy("lb", "pB", 1))
	p.SetGroups([]config.GroupCfg{{Name: "g", MinAliveRatio: 0, Primary: []string{"pA", "pB"}}})

	aCount, bCount := 0, 0
	total := 20000
	for range total {
		pr := p.NextInGroup("g")
		if pr == nil {
			t.Fatal("NextInGroup returned nil")
		}
		if pr.Provider == "pA" {
			aCount++
		} else {
			bCount++
		}
	}
	_ = bCount
	ratio := float64(aCount) / float64(total)
	if ratio < 0.73 || ratio > 0.77 {
		t.Fatalf("expected ~0.75 SWRR ratio, got %f", ratio)
	}
}

// TestGroupLatencyAwareSWRR verifies a faster proxy receives a higher share of
// traffic while a slower one stays in rotation, and that unknown latency keeps
// plain SWRR behaviour.
func TestGroupLatencyAwareSWRR(t *testing.T) {
	p := NewPool()
	// same base weight but very different latency -> faster proxy should win
	p.Add(newGroupProxy("slow", "pA", 1))
	p.Add(newGroupProxy("fast", "pB", 1))
	// pool-based kinds enable SWRR
	p.Get("slow").Kind = model.KindSticky
	p.Get("fast").Kind = model.KindSticky
	p.SetGroups([]config.GroupCfg{{Name: "g", MinAliveRatio: 0, Primary: []string{"pA", "pB"}}})

	slow := p.Get("slow")
	fast := p.Get("fast")
	slow.LatencyMS.Store(2000) // 2s
	fast.LatencyMS.Store(200)  // 200ms

	fastCount := 0
	total := 20000
	for range total {
		pr := p.NextInGroup("g")
		if pr == nil {
			t.Fatal("NextInGroup returned nil")
		}
		if pr.Provider == "pB" {
			fastCount++
		}
	}
	ratio := float64(fastCount) / float64(total)
	if ratio < 0.6 {
		t.Fatalf("expected fast proxy to dominate (>=0.6), got %f", ratio)
	}
}

func TestGroupTunnelUsesRandomNotSWRR(t *testing.T) {
	p := NewPool()
	// mixed tunnel + sticky in one tier disables SWRR
	p.Add(newGroupProxy("tun", "pA", 1))
	p.Add(newGroupProxy("sta", "pB", 1))
	// force pB to a pool-based kind
	pr := p.Get("sta")
	pr.Kind = model.KindSticky
	p.SetGroups([]config.GroupCfg{{Name: "g", MinAliveRatio: 0, Primary: []string{"pA", "pB"}}})

	seen := map[string]int{}
	for range 10000 {
		pr := p.NextInGroup("g")
		if pr == nil {
			t.Fatal("NextInGroup returned nil")
		}
		seen[pr.ID]++
	}
	if seen["tun"] == 0 || seen["sta"] == 0 {
		t.Fatalf("expected both proxies used, got %v", seen)
	}
}

func TestGroupStats(t *testing.T) {
	p := NewPool()
	p.Add(newGroupProxy("a", "pA", 1))
	p.Add(newGroupProxy("b", "pB", 1))
	p.SetGroups([]config.GroupCfg{
		{Name: "g", MinAliveRatio: 0.5, Primary: []string{"pA"}, Backups: []config.BackupPool{{Name: "bk", MinAliveRatio: 0, Providers: []string{"pB"}}}},
	})
	stats := p.GroupStats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 group, got %d", len(stats))
	}
	if stats[0].Group != "g" || len(stats[0].Tiers) != 2 {
		t.Fatalf("unexpected stats: %+v", stats[0])
	}
}

func TestRegionMatches(t *testing.T) {
	cases := []struct {
		regions []string
		country string
		want    bool
	}{
		{nil, "CN", true},
		{nil, "US", true},
		{[]string{}, "CN", true},
		{[]string{"domestic"}, "CN", true},
		{[]string{"domestic"}, "US", false},
		{[]string{"domestic"}, "HK", false},
		{[]string{"overseas"}, "US", true},
		{[]string{"overseas"}, "HK", true},
		{[]string{"overseas"}, "CN", false},
		{[]string{"overseas"}, "", true},
		{[]string{"US"}, "us", true},
		{[]string{"US"}, "CN", false},
		{[]string{"US", "JP"}, "JP", true},
		{[]string{"US", "JP"}, "HK", false},
		{[]string{"domestic", "US"}, "US", true},
		{[]string{"domestic", "US"}, "CN", true},
		{[]string{"domestic", "US"}, "JP", false},
	}
	for _, c := range cases {
		if got := regionMatches(c.regions, c.country); got != c.want {
			t.Errorf("regionMatches(%v, %q) = %v, want %v", c.regions, c.country, got, c.want)
		}
	}
}

func TestGroupRegionFilter(t *testing.T) {
	p := NewPool()
	cn := newTestProxy("cn1", 1)
	cn.Country = "CN"
	us := newTestProxy("us1", 1)
	us.Country = "US"
	hk := newTestProxy("hk1", 1)
	hk.Country = "HK"
	p.Add(cn)
	p.Add(us)
	p.Add(hk)

	// domestic only: only CN proxy usable
	p.SetGroups([]config.GroupCfg{{Name: "domestic", MinAliveRatio: 0, Primary: []string{"test"}, Regions: []string{"domestic"}}})
	g := p.GroupStats()
	if len(g) != 1 {
		t.Fatalf("expected 1 group, got %d", len(g))
	}
	tier := g[0].Tiers[0]
	if tier.AliveCount != 1 {
		t.Fatalf("domestic alive_count = %d, want 1 (only CN)", tier.AliveCount)
	}
	if pr := p.NextInGroup("domestic"); pr == nil || pr.ID != "cn1" {
		t.Fatalf("domestic group should pick cn1, got %v", pr)
	}

	// overseas: US + HK usable, CN excluded
	p.SetGroups([]config.GroupCfg{{Name: "overseas", MinAliveRatio: 0, Primary: []string{"test"}, Regions: []string{"overseas"}}})
	tier = p.GroupStats()[0].Tiers[0]
	if tier.AliveCount != 2 {
		t.Fatalf("overseas alive_count = %d, want 2 (US+HK)", tier.AliveCount)
	}

	// mixed domestic + US
	p.SetGroups([]config.GroupCfg{{Name: "mix", MinAliveRatio: 0, Primary: []string{"test"}, Regions: []string{"domestic", "US"}}})
	tier = p.GroupStats()[0].Tiers[0]
	if tier.AliveCount != 2 {
		t.Fatalf("mix alive_count = %d, want 2 (CN+US)", tier.AliveCount)
	}

	// no regions: all three usable
	p.SetGroups([]config.GroupCfg{{Name: "all", MinAliveRatio: 0, Primary: []string{"test"}}})
	tier = p.GroupStats()[0].Tiers[0]
	if tier.AliveCount != 3 {
		t.Fatalf("all alive_count = %d, want 3", tier.AliveCount)
	}
}

// TestGroupPrimaryWeights verifies that the group-level primary_weights
// override the per-proxy weight when building a group's scheduling tier.
func TestGroupPrimaryWeights(t *testing.T) {
	p := NewPool()
	a := newTestProxy("a1", 1)
	a.Provider = "pa"
	b := newTestProxy("b1", 1)
	b.Provider = "pb"
	p.Add(a)
	p.Add(b)

	p.SetGroups([]config.GroupCfg{{
		Name:           "g",
		MinAliveRatio:  0,
		Primary:        []string{"pa", "pb"},
		PrimaryWeights: map[string]int{"pa": 3, "pb": 1},
	}})

	snap := p.Snapshot()
	if len(snap.groups) != 1 || len(snap.groups[0].layers) != 1 {
		t.Fatalf("expected 1 group with 1 layer, got %d/%d", len(snap.groups), len(snap.groups[0].layers))
	}
	l := snap.groups[0].layers[0]
	if l.totalW != 4 {
		t.Fatalf("layer totalW = %d, want 4 (pa*3 + pb*1)", l.totalW)
	}
	if l.cumW[0] != 3 {
		t.Fatalf("cumW[0] = %d, want 3 (pa weighted)", l.cumW[0])
	}
}

// TestGroupPrimaryWeightsIgnoredForAbsentProvider verifies that providers not
// listed in primary_weights keep their own proxy weight.
func TestGroupPrimaryWeightsAbsentProviderKeepsOwnWeight(t *testing.T) {
	p := NewPool()
	a := newTestProxy("a1", 5)
	a.Provider = "pa"
	p.Add(a)

	p.SetGroups([]config.GroupCfg{{
		Name:           "g",
		MinAliveRatio:  0,
		Primary:        []string{"pa"},
		PrimaryWeights: map[string]int{"other": 9},
	}})

	snap := p.Snapshot()
	l := snap.groups[0].layers[0]
	if l.totalW != 5 {
		t.Fatalf("layer totalW = %d, want 5 (own weight preserved)", l.totalW)
	}
}
