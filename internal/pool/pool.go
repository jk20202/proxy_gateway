package pool

import (
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

type Snapshot struct {
	proxies []*model.Proxy
	prio    []prioGroup // legacy priority groups (used when no explicit groups configured)
	groups  []groupSnap // explicit named groups with layered fallback
}

// prioGroup is the legacy priority-based scheduling unit.
type prioGroup struct {
	priority   int
	start, end int // 在 proxies 中的区间
	aliveTotal int // 该组总代理数（含 dead）
	aliveCount int // 该组存活代理数
	minRatio   float64
	cumW       []int64
	totalW     int64
}

// groupSnap is an explicit named group with an ordered list of fallback tiers.
type groupSnap struct {
	name   string
	layers []layerSnap
}

// layerSnap is one scheduling tier of a group: either the primary pool or a
// single backup pool. It aggregates proxies from all providers in that tier.
type layerSnap struct {
	name       string
	proxies    []*model.Proxy
	aliveTotal int // 该层全部代理数（含 dead）
	aliveCount int // 该层存活代理数
	minRatio   float64
	useSWRR    bool // 层内全是池化型（静态/IP池）时用平滑加权轮询
	cumW       []int64
	totalW     int64
}

func (s *Snapshot) Proxies() []*model.Proxy {
	if s == nil {
		return nil
	}
	return s.proxies
}

type Pool struct {
	mu      sync.RWMutex
	proxies map[string]*model.Proxy

	snap atomic.Pointer[Snapshot]
	seq  atomic.Uint64

	stickyMu sync.Mutex
	sticky   map[string]stickyEntry

	groupsCfg []config.GroupCfg
	swrrMu    sync.Mutex
	swrr      map[string]int64 // proxyID -> current weight (SWRR)

	stateSinkMu sync.RWMutex
	stateSink   func(pr *model.Proxy, removed bool)
}

type stickyEntry struct {
	proxyID  string
	expireAt int64 // unix nano
}

func NewPool() *Pool {
	p := &Pool{
		proxies: make(map[string]*model.Proxy),
		sticky:  make(map[string]stickyEntry),
		swrr:    make(map[string]int64),
	}
	p.snap.Store(&Snapshot{})
	return p
}

// SetStateSink registers a callback invoked whenever a proxy's runtime state
// changes (added, removed, latency/alive/country updated). It is used to mirror
// high-frequency proxy state into Redis. Passing nil disables it.
func (p *Pool) SetStateSink(fn func(pr *model.Proxy, removed bool)) {
	p.stateSinkMu.Lock()
	p.stateSink = fn
	p.stateSinkMu.Unlock()
}

// notifyState fires the state sink for a proxy. Best-effort and non-blocking.
func (p *Pool) notifyState(pr *model.Proxy, removed bool) {
	p.stateSinkMu.RLock()
	fn := p.stateSink
	p.stateSinkMu.RUnlock()
	if fn != nil && pr != nil {
		fn(pr, removed)
	}
}

// SetGroups installs the explicit group scheduling configuration and rebuilds
// the snapshot. An empty slice restores the legacy priority mechanism.
func (p *Pool) SetGroups(cfg []config.GroupCfg) {
	p.groupsCfg = cfg
	p.Rebuild()
}

// Groups returns the configured group definitions.
func (p *Pool) Groups() []config.GroupCfg {
	return p.groupsCfg
}

func (p *Pool) Add(pr *model.Proxy) {
	if pr.Weight <= 0 {
		pr.Weight = 1
	}
	p.mu.Lock()
	if old, ok := p.proxies[pr.ID]; ok {
		old.Host = pr.Host
		old.Port = pr.Port
		old.Username = pr.Username
		old.Password = pr.Password
		old.Scheme = pr.Scheme
		old.Provider = pr.Provider
		old.Kind = pr.Kind
		old.Weight = pr.Weight
		old.CheckURL = pr.CheckURL
		old.CheckURLs = pr.CheckURLs
		old.Free = pr.Free
		old.Priority = pr.Priority
		old.MinAliveRatio = pr.MinAliveRatio
		old.StickySeconds = pr.StickySeconds
		p.mu.Unlock()
		p.notifyState(old, false)
		return
	}
	p.proxies[pr.ID] = pr
	p.mu.Unlock()
	p.Rebuild()
	p.notifyState(pr, false)
}

func (p *Pool) Remove(id string) bool {
	p.mu.Lock()
	pr, ok := p.proxies[id]
	if ok {
		delete(p.proxies, id)
		p.mu.Unlock()
		p.ClearSticky(id)
		p.clearSWRR(id)
		p.Rebuild()
		p.notifyState(pr, true)
		return true
	}
	p.mu.Unlock()
	return false
}

// clearSWRR removes SWRR state for a proxy id.
func (p *Pool) clearSWRR(id string) {
	p.swrrMu.Lock()
	delete(p.swrr, id)
	p.swrrMu.Unlock()
}

func (p *Pool) Get(id string) *model.Proxy {
	p.mu.RLock()
	pr := p.proxies[id]
	p.mu.RUnlock()
	return pr
}

// UnresolvedCountries returns the proxy IDs that still have an empty country,
// grouped by their host address. Providers that explicitly set a country skip
// enrichment and are never returned here.
func (p *Pool) UnresolvedCountries() map[string][]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string][]string)
	for id, pr := range p.proxies {
		if pr.Country == "" {
			out[pr.Host] = append(out[pr.Host], id)
		}
	}
	return out
}

// SetCountry records the resolved country code for the given proxy id. It
// rebuilds the snapshot when the value actually changed (region filters
// depend on it).
func (p *Pool) SetCountry(id, country string) {
	p.mu.Lock()
	pr, ok := p.proxies[id]
	if ok && pr.Country != country {
		pr.Country = country
		p.mu.Unlock()
		p.Rebuild()
		p.notifyState(pr, false)
		return
	}
	p.mu.Unlock()
}

func (p *Pool) All() []*model.Proxy {
	p.mu.RLock()
	out := make([]*model.Proxy, 0, len(p.proxies))
	for _, pr := range p.proxies {
		out = append(out, pr)
	}
	p.mu.RUnlock()
	return out
}

func (p *Pool) Snapshot() *Snapshot {
	return p.snap.Load()
}

type gStat struct {
	total    int
	alive    int
	minRatio float64
}

func (p *Pool) Rebuild() {
	p.mu.RLock()
	stats := map[int]*gStat{}
	alive := make([]*model.Proxy, 0, len(p.proxies))
	for _, pr := range p.proxies {
		st := stats[pr.Priority]
		if st == nil {
			st = &gStat{minRatio: 1.0}
			stats[pr.Priority] = st
		}
		st.total++
		if pr.MinAliveRatio < st.minRatio {
			st.minRatio = pr.MinAliveRatio
		}
		if pr.Alive.Load() {
			st.alive++
			alive = append(alive, pr)
		}
	}
	p.mu.RUnlock()

	snap := &Snapshot{proxies: alive}
	if len(p.groupsCfg) > 0 {
		snap.groups = p.buildGroups(alive)
	} else {
		snap.prio = buildPrioGroups(alive, stats)
	}
	p.snap.Store(snap)
	p.seq.Add(1)
}

// buildPrioGroups sorts alive proxies by priority and builds legacy groups.
func buildPrioGroups(alive []*model.Proxy, stats map[int]*gStat) []prioGroup {
	sort.Slice(alive, func(i, j int) bool {
		if alive[i].Priority != alive[j].Priority {
			return alive[i].Priority > alive[j].Priority
		}
		return alive[i].ID < alive[j].ID
	})

	groups := make([]prioGroup, 0, len(stats))
	i := 0
	for i < len(alive) {
		prio := alive[i].Priority
		st := stats[prio]
		g := prioGroup{
			priority:   prio,
			start:      i,
			aliveTotal: st.total,
			aliveCount: st.alive,
			minRatio:   st.minRatio,
		}
		var acc int64
		for i < len(alive) && alive[i].Priority == prio {
			acc += int64(alive[i].Weight)
			g.cumW = append(g.cumW, acc)
			i++
		}
		g.end = i
		g.totalW = acc
		groups = append(groups, g)
	}
	return groups
}

// buildGroups builds explicit group snapshots from the configured group
// definitions. Each group's primary tier plus every backup tier is computed
// from the providers it references. alive holds the currently alive proxies.
func (p *Pool) buildGroups(alive []*model.Proxy) []groupSnap {
	p.mu.RLock()
	all := make([]*model.Proxy, 0, len(p.proxies))
	for _, pr := range p.proxies {
		all = append(all, pr)
	}
	p.mu.RUnlock()

	out := make([]groupSnap, 0, len(p.groupsCfg))
	for _, gc := range p.groupsCfg {
		g := groupSnap{name: gc.Name}
		totalByProv, aliveByProv := regionAggregates(gc.Regions, all, alive)
		g.layers = append(g.layers, buildLayer("primary", gc.Primary, gc.PrimaryWeights, gc.MinAliveRatio, aliveByProv, totalByProv))
		for _, bk := range gc.Backups {
			g.layers = append(g.layers, buildLayer(bk.Name, bk.Providers, nil, bk.MinAliveRatio, aliveByProv, totalByProv))
		}
		out = append(out, g)
	}
	return out
}

// regionAggregates builds per-provider total/alive aggregations containing
// only proxies whose country matches at least one region filter. When no
// regions are configured every proxy is kept.
func regionAggregates(regions []string, all []*model.Proxy, alive []*model.Proxy) (total map[string]int, aliveProv map[string][]*model.Proxy) {
	total = make(map[string]int, len(all))
	aliveProv = make(map[string][]*model.Proxy, len(alive))
	for _, pr := range all {
		if regionMatches(regions, pr.Country) {
			total[pr.Provider]++
		}
	}
	for _, pr := range alive {
		if regionMatches(regions, pr.Country) {
			aliveProv[pr.Provider] = append(aliveProv[pr.Provider], pr)
		}
	}
	return total, aliveProv
}

// regionMatches reports whether a proxy's country satisfies the group's
// region filter. "domestic" matches 中国内地 (CN); "overseas" matches
// everything outside 中国内地, including 中国香港 (HK) and unknown countries.
// An empty filter matches every proxy. Empty country counts as overseas.
func regionMatches(regions []string, country string) bool {
	if len(regions) == 0 {
		return true
	}
	for _, r := range regions {
		switch r {
		case "domestic":
			if country == "CN" {
				return true
			}
		case "overseas":
			if country != "CN" {
				return true
			}
		default:
			if strings.EqualFold(country, r) {
				return true
			}
		}
	}
	return false
}

// buildLayer aggregates the alive proxies from the given provider names and
// computes cumulative weights. aliveTotal counts proxies including dead ones.
// SWRR is used only when the whole tier consists of pool-based proxies
// (sticky / ip_pool); tiers containing tunnel proxies use weighted random.
// weights overrides the per-provider weight (provider name -> weight, applied
// to every proxy from that provider); nil/absent means use the proxy's own
// weight. This is where the "主池 Provider 权重" configured on a group is
// applied: weight has been migrated from the Provider layer to the group's
// primary pool layer.
func buildLayer(name string, providers []string, weights map[string]int, minRatio float64, aliveByProv map[string][]*model.Proxy, totalByProv map[string]int) layerSnap {
	l := layerSnap{name: name, minRatio: minRatio, useSWRR: true}
	seen := map[string]bool{}
	for _, pn := range providers {
		l.aliveTotal += totalByProv[pn]
		for _, pr := range aliveByProv[pn] {
			if seen[pr.ID] {
				continue
			}
			seen[pr.ID] = true
			l.proxies = append(l.proxies, pr)
			l.aliveCount++
			if pr.Kind == model.KindTunnel {
				l.useSWRR = false
			}
		}
	}
	var acc int64
	for _, pr := range l.proxies {
		w := pr.Weight
		if weights != nil {
			if gw, ok := weights[pr.Provider]; ok && gw > 0 {
				w = int32(gw)
			}
		}
		if w <= 0 {
			w = 1
		}
		acc += int64(w)
		l.cumW = append(l.cumW, acc)
	}
	l.totalW = acc
	return l
}

func (p *Pool) Seq() uint64 {
	return p.seq.Load()
}

// Next returns a proxy using the configured scheduling. With explicit groups
// configured, it uses the first group; otherwise the legacy priority logic.
func (p *Pool) Next() *model.Proxy {
	snap := p.snap.Load()
	if len(snap.groups) > 0 {
		return p.nextFromGroup(&snap.groups[0], nil)
	}
	return p.nextPrio(snap, nil)
}

// NextInGroup returns a proxy from the named group, falling back through the
// group's backup tiers. Returns nil when the group is unknown or exhausted.
func (p *Pool) NextInGroup(group string) *model.Proxy {
	snap := p.snap.Load()
	for i := range snap.groups {
		if snap.groups[i].name == group {
			return p.nextFromGroup(&snap.groups[i], nil)
		}
	}
	return nil
}

// NextInGroupExcluding returns a proxy from the named group, excluding the
// given proxy IDs (used by batch allocation).
func (p *Pool) NextInGroupExcluding(group string, exclude map[string]struct{}) *model.Proxy {
	if len(exclude) == 0 {
		return p.NextInGroup(group)
	}
	snap := p.snap.Load()
	for i := range snap.groups {
		if snap.groups[i].name != group {
			continue
		}
		g := &snap.groups[i]
		// try to find a non-excluded proxy across the tier chain
		var firstDown bool
		for li := range g.layers {
			l := &g.layers[li]
			if l.aliveCount == 0 {
				continue
			}
			if float64(l.aliveCount)/float64(l.aliveTotal) < l.minRatio {
				firstDown = true
				continue
			}
			pr := p.pickLayer(l, exclude)
			if pr != nil {
				return pr
			}
		}
		_ = firstDown
		return nil
	}
	return nil
}

func (p *Pool) nextFromGroup(g *groupSnap, exclude map[string]struct{}) *model.Proxy {
	for li := range g.layers {
		l := &g.layers[li]
		if l.aliveCount == 0 {
			continue
		}
		if float64(l.aliveCount)/float64(l.aliveTotal) < l.minRatio {
			continue
		}
		pr := p.pickLayer(l, exclude)
		if pr != nil {
			return pr
		}
	}
	return nil
}

// pickLayer selects a proxy within one tier using SWRR for pool-based tiers
// and weighted random otherwise.
func (p *Pool) pickLayer(l *layerSnap, exclude map[string]struct{}) *model.Proxy {
	if l.totalW <= 0 || len(l.proxies) == 0 {
		return nil
	}
	if len(exclude) == 0 {
		if l.useSWRR {
			return p.pickSWRR(l.proxies)
		}
		target := rand.Int64N(l.totalW)
		j := sort.Search(len(l.cumW), func(i int) bool { return l.cumW[i] > target })
		return l.proxies[j]
	}
	// with exclusions: scan for a non-excluded proxy
	if l.useSWRR {
		// try up to len(proxies) rounds to find an unexcluded one
		for range len(l.proxies) {
			pr := p.pickSWRR(l.proxies)
			if _, ok := exclude[pr.ID]; !ok {
				return pr
			}
		}
		return nil
	}
	for range 1000 {
		target := rand.Int64N(l.totalW)
		j := sort.Search(len(l.cumW), func(i int) bool { return l.cumW[i] > target })
		pr := l.proxies[j]
		if _, ok := exclude[pr.ID]; !ok {
			return pr
		}
	}
	return nil
}

// pickSWRR performs smooth weighted round-robin selection over the given
// proxies. SWRR state is tracked per proxy ID.
func (p *Pool) pickSWRR(proxies []*model.Proxy) *model.Proxy {
	p.swrrMu.Lock()
	defer p.swrrMu.Unlock()

	var total int64
	var best *model.Proxy
	var bestCW int64 = math.MinInt64
	for _, pr := range proxies {
		cw := p.swrr[pr.ID] + int64(pr.Weight)
		p.swrr[pr.ID] = cw
		total += int64(pr.Weight)
		if cw > bestCW {
			bestCW = cw
			best = pr
		}
	}
	if best == nil || total == 0 {
		return nil
	}
	p.swrr[best.ID] -= total
	return best
}

func (p *Pool) nextPrio(snap *Snapshot, exclude map[string]struct{}) *model.Proxy {
	for gi := range snap.prio {
		g := &snap.prio[gi]
		if g.aliveCount == 0 {
			continue
		}
		if float64(g.aliveCount)/float64(g.aliveTotal) < g.minRatio {
			continue
		}
		for range 1000 {
			target := rand.Int64N(g.totalW)
			j := sort.Search(len(g.cumW), func(i int) bool { return g.cumW[i] > target })
			pr := snap.proxies[g.start+j]
			if _, ok := exclude[pr.ID]; !ok {
				return pr
			}
		}
	}
	return nil
}

func (p *Pool) NextExcluding(exclude map[string]struct{}) *model.Proxy {
	if len(exclude) == 0 {
		return p.Next()
	}
	snap := p.snap.Load()
	if len(snap.groups) > 0 {
		return p.NextInGroupExcluding(snap.groups[0].name, exclude)
	}
	return p.nextPrio(snap, exclude)
}

// StickyNext returns a proxy for clientKey, reusing the same proxy until the
// sticky window (seconds) expires. If stickySeconds <= 0 or no valid sticky
// session exists, it falls back to Next() (or NextInGroup when group is set)
// and records a new session when the selected proxy supports stickiness.
func (p *Pool) StickyNext(clientKey string, stickySeconds int, group string) *model.Proxy {
	if clientKey == "" || stickySeconds <= 0 {
		if group != "" {
			return p.NextInGroup(group)
		}
		return p.Next()
	}

	now := time.Now().UnixNano()
	window := int64(stickySeconds) * int64(time.Second)

	p.stickyMu.Lock()
	if e, ok := p.sticky[clientKey]; ok && e.expireAt > now {
		// session still valid: reuse if proxy still alive and present
		pr := p.Get(e.proxyID)
		if pr != nil && pr.Alive.Load() {
			p.stickyMu.Unlock()
			return pr
		}
		// stale session: drop and re-pick below
		delete(p.sticky, clientKey)
	}
	p.stickyMu.Unlock()

	var pr *model.Proxy
	if group != "" {
		pr = p.NextInGroup(group)
	} else {
		pr = p.Next()
	}
	if pr == nil {
		return nil
	}
	p.stickyMu.Lock()
	p.sticky[clientKey] = stickyEntry{proxyID: pr.ID, expireAt: now + window}
	if len(p.sticky) > 4096 {
		for k, e := range p.sticky {
			if e.expireAt <= now {
				delete(p.sticky, k)
			}
		}
	}
	p.stickyMu.Unlock()
	return pr
}

// ClearSticky removes a client's sticky session (e.g. admin delete proxy).
func (p *Pool) ClearSticky(proxyID string) {
	p.stickyMu.Lock()
	defer p.stickyMu.Unlock()
	for k, e := range p.sticky {
		if e.proxyID == proxyID {
			delete(p.sticky, k)
		}
	}
}

func (p *Pool) MarkFailed(id string, latencyMS int64) {
	if pr := p.Get(id); pr != nil {
		f := pr.Fails.Add(1)
		if latencyMS > 0 {
			pr.LatencyMS.Store(latencyMS)
		}
		if f >= 3 {
			pr.Alive.Store(false)
			p.Rebuild()
		}
		p.notifyState(pr, false)
	}
}

func (p *Pool) MarkSuccess(id string, latencyMS int64) {
	if pr := p.Get(id); pr != nil {
		pr.Fails.Store(0)
		if latencyMS > 0 {
			pr.LatencyMS.Store(latencyMS)
		}
		pr.LastUsed.Store(time.Now().UnixNano())
		if !pr.Alive.Load() {
			pr.Alive.Store(true)
			p.Rebuild()
		}
		p.notifyState(pr, false)
	}
}

func (p *Pool) SetAlive(id string, alive bool) {
	if pr := p.Get(id); pr != nil {
		pr.Alive.Store(alive)
		p.Rebuild()
		p.notifyState(pr, false)
	}
}

func (p *Pool) RemoveExpired() int {
	now := time.Now().UnixNano()
	var removed int
	p.mu.Lock()
	for id, pr := range p.proxies {
		if pr.ExpireAt > 0 && pr.ExpireAt < now {
			delete(p.proxies, id)
			removed++
		}
	}
	p.mu.Unlock()
	if removed > 0 {
		p.Rebuild()
	}
	return removed
}

func (p *Pool) Stats() (total, alive int) {
	p.mu.RLock()
	total = len(p.proxies)
	p.mu.RUnlock()
	alive = len(p.snap.Load().proxies)
	return total, alive
}

type ProviderStat struct {
	Provider string
	Total    int
	Alive    int
}

// GroupStat reports the per-tier status of a scheduling group.
type GroupStat struct {
	Group string     `json:"group"`
	Type  string     `json:"type"`
	Tiers []TierStat `json:"tiers"`
}

// TierStat is one tier (primary or backup) of a group.
type TierStat struct {
	Name       string  `json:"name"`
	AliveTotal int     `json:"alive_total"`
	AliveCount int     `json:"alive_count"`
	MinRatio   float64 `json:"min_alive_ratio"`
	Usable     bool    `json:"usable"`
}

// GroupStats returns the scheduling status of all configured groups.
func (p *Pool) GroupStats() []GroupStat {
	snap := p.snap.Load()
	out := make([]GroupStat, 0, len(snap.groups))
	for gi := range snap.groups {
		g := &snap.groups[gi]
		gs := GroupStat{Group: g.name, Type: groupType(p.groupsCfg, g.name)}
		for li := range g.layers {
			l := &g.layers[li]
			usable := l.aliveCount > 0 && float64(l.aliveCount)/float64(l.aliveTotal) >= l.minRatio
			gs.Tiers = append(gs.Tiers, TierStat{
				Name:       l.name,
				AliveTotal: l.aliveTotal,
				AliveCount: l.aliveCount,
				MinRatio:   l.minRatio,
				Usable:     usable,
			})
		}
		out = append(out, gs)
	}
	return out
}

func groupType(cfgs []config.GroupCfg, name string) string {
	for _, c := range cfgs {
		if c.Name == name {
			return c.Type
		}
	}
	return ""
}

func (p *Pool) StatsByProvider() []ProviderStat {
	byProv := map[string]*ProviderStat{}
	p.mu.RLock()
	for _, pr := range p.proxies {
		st := byProv[pr.Provider]
		if st == nil {
			st = &ProviderStat{Provider: pr.Provider}
			byProv[pr.Provider] = st
		}
		st.Total++
	}
	p.mu.RUnlock()
	for _, pr := range p.snap.Load().proxies {
		if st := byProv[pr.Provider]; st != nil {
			st.Alive++
		}
	}
	out := make([]ProviderStat, 0, len(byProv))
	for _, st := range byProv {
		out = append(out, *st)
	}
	return out
}

type ProxyDetail struct {
	ID            string  `json:"id"`
	Provider      string  `json:"provider"`
	Kind          string  `json:"kind"`
	Scheme        string  `json:"scheme"`
	Addr          string  `json:"addr"`
	Username      string  `json:"username"`
	Alive         bool    `json:"alive"`
	Fails         int32   `json:"fails"`
	LatencyMS     int64   `json:"latency_ms"`
	Weight        int32   `json:"weight"`
	Priority      int     `json:"priority"`
	MinAliveRatio float64 `json:"min_alive_ratio"`
	StickySeconds int     `json:"sticky_seconds"`
	Free          bool    `json:"free"`
	Country       string  `json:"country"`
	ExpireAt      int64   `json:"expire_at"`
}

func (p *Pool) ProxyList() []ProxyDetail {
	p.mu.RLock()
	out := make([]ProxyDetail, 0, len(p.proxies))
	for _, pr := range p.proxies {
		out = append(out, ProxyDetail{
			ID:            pr.ID,
			Provider:      pr.Provider,
			Kind:          pr.Kind.String(),
			Scheme:        pr.Scheme,
			Addr:          pr.Addr(),
			Username:      pr.Username,
			Alive:         pr.Alive.Load(),
			Fails:         pr.Fails.Load(),
			LatencyMS:     pr.LatencyMS.Load(),
			Weight:        pr.Weight,
			Priority:      pr.Priority,
			MinAliveRatio: pr.MinAliveRatio,
			StickySeconds: pr.StickySeconds,
			Free:          pr.Free,
			Country:       pr.Country,
			ExpireAt:      pr.ExpireAt,
		})
	}
	p.mu.RUnlock()
	return out
}

func (p *Pool) RemoveByProvider(provider string) int {
	p.mu.Lock()
	ids := make([]string, 0)
	for id, pr := range p.proxies {
		if pr.Provider == provider {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		delete(p.proxies, id)
	}
	p.mu.Unlock()
	if len(ids) > 0 {
		p.swrrMu.Lock()
		for _, id := range ids {
			delete(p.swrr, id)
		}
		p.swrrMu.Unlock()
		for _, id := range ids {
			p.ClearSticky(id)
		}
		p.Rebuild()
	}
	return len(ids)
}
