package provider

import (
	"context"
	"fmt"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

type Tunnel struct {
	cfg config.ProviderCfg
}

func NewTunnel(cfg config.ProviderCfg) *Tunnel {
	return &Tunnel{cfg: cfg}
}

func (t *Tunnel) Name() string     { return t.cfg.Name }
func (t *Tunnel) Kind() model.Kind { return model.KindTunnel }

func (t *Tunnel) Weight() int32 {
	return int32(t.cfg.Weight)
}

func (t *Tunnel) CheckURL() string {
	if t.cfg.CheckURL != "" {
		return t.cfg.CheckURL
	}
	return ""
}

func (t *Tunnel) Initial(ctx context.Context) ([]*model.Proxy, error) {
	tc := t.cfg.Tunnel
	pr := &model.Proxy{
		ID:              fmt.Sprintf("%s:%s:%d", t.cfg.Name, tc.Gateway, tc.Port),
		Provider:        t.cfg.Name,
		Kind:            model.KindTunnel,
		Scheme:          tc.Scheme,
		Host:            tc.Gateway,
		Port:            tc.Port,
		Username:        tc.Username,
		Password:        tc.Password,
		Weight:          int32(t.cfg.Weight),
		CheckURL:        t.CheckURL(),
		CheckIntervalMS: int64(t.cfg.CheckIntervalS) * 1000,
		Priority:        t.cfg.Priority,
		MinAliveRatio:   t.cfg.MinAliveRatio,
	}
	pr.Alive.Store(true)
	return []*model.Proxy{pr}, nil
}

func (t *Tunnel) Refresh(ctx context.Context) ([]*model.Proxy, error) {
	return nil, nil
}
