package provider

import (
	"context"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

type Provider interface {
	Name() string
	Kind() model.Kind
	Weight() int32
	CheckURL() string
	Initial(ctx context.Context) ([]*model.Proxy, error)
	Refresh(ctx context.Context) ([]*model.Proxy, error)
}

func New(cfg config.ProviderCfg) (Provider, error) {
	switch cfg.Type {
	case "tunnel":
		return NewTunnel(cfg), nil
	case "ip_pool", "sticky":
		return NewIPPool(cfg), nil
	case "free":
		return NewFreePool(cfg), nil
	}
	return nil, &config.UnsupportedTypeError{Type: cfg.Type}
}
