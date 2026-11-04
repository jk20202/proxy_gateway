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
	// 直接粘贴 URL 列表（tunnel 动态隧道 / sticky 静态隧道）优先：一个 URL 一条
	// 代理，支持多套餐多 URL。
	if len(cfg.URLs) > 0 && (cfg.Type == "tunnel" || cfg.Type == "sticky") {
		return NewStaticURLs(cfg), nil
	}
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
