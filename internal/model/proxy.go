package model

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Kind uint8

const (
	KindTunnel Kind = iota + 1
	KindIPPool
	KindSticky
)

func (k Kind) String() string {
	switch k {
	case KindTunnel:
		return "tunnel"
	case KindIPPool:
		return "ip_pool"
	case KindSticky:
		return "sticky"
	}
	return "unknown"
}

type Proxy struct {
	ID       string
	Provider string
	Kind     Kind

	Scheme   string
	Host     string
	Port     int
	Username string
	Password string

	Weight   int32
	CheckURL string
	ExpireAt int64

	// CheckIntervalMS 检测频率：池内 IP 多久检测一次延迟。0=使用全局 health_check.interval_s。
	CheckIntervalMS int64

	Priority      int     // 越大越优先，选代理时优先使用高优先级组
	MinAliveRatio float64 // 本 provider 存活率阈值：存活比例 >= 此值才使用本优先级组，否则降级
	StickySeconds int     // 粘性时长（秒）：>0 时同一客户端在此时长内复用同一代理

	Free bool // 免费代理：一次检测失败或延迟>5s 直接删除，不做重试保留

	DeleteLatencyMS int64 // 免费代理健康检测延迟超过该值(ms)直接删除；0=默认3000

	Country string // ISO 3166-1 alpha-2 国家代码（CN=中国内地，HK=中国香港，其余=海外）；空=未知

	Alive     atomic.Bool
	Fails     atomic.Int32
	LatencyMS atomic.Int64
	LastUsed  atomic.Int64
}

func (p *Proxy) Addr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

func (p *Proxy) Clone() *Proxy {
	c := &Proxy{
		ID:              p.ID,
		Provider:        p.Provider,
		Kind:            p.Kind,
		Scheme:          p.Scheme,
		Host:            p.Host,
		Port:            p.Port,
		Username:        p.Username,
		Password:        p.Password,
		Weight:          p.Weight,
		CheckURL:        p.CheckURL,
		ExpireAt:        p.ExpireAt,
		CheckIntervalMS: p.CheckIntervalMS,
		Priority:        p.Priority,
		MinAliveRatio:   p.MinAliveRatio,
		StickySeconds:   p.StickySeconds,
		Free:            p.Free,
		DeleteLatencyMS: p.DeleteLatencyMS,
		Country:         p.Country,
	}
	c.Alive.Store(p.Alive.Load())
	c.Fails.Store(p.Fails.Load())
	c.LatencyMS.Store(p.LatencyMS.Load())
	c.LastUsed.Store(p.LastUsed.Load())
	return c
}

func (p *Proxy) LastUsedTime() time.Time {
	return time.Unix(0, p.LastUsed.Load())
}
