package persist

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"proxy-pool/internal/config"
	"proxy-pool/internal/model"
)

// ProxyState is the persisted runtime snapshot of one proxy (latency / country
// / alive) kept in Redis so high-frequency health data survives restarts
// without touching the relational database.
type ProxyState struct {
	LatencyMS int64  `json:"l"`
	Country   string `json:"c"`
	Alive     bool   `json:"a"`
	Ts        int64  `json:"t"`
}

// Redis caches per-proxy runtime state. Every write is fire-and-forget with a
// short timeout so the hot health-check path never blocks on the network.
type Redis struct {
	client *redis.Client
	logger *slog.Logger
}

func proxyKey(provider string) string {
	return "pp:proxy:" + provider
}

// OpenRedis connects to Redis. Returns nil when not configured.
func OpenRedis(cfg config.RedisConfig, logger *slog.Logger) (*Redis, error) {
	addr := cfg.Addr
	if addr == "" {
		return nil, nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{client: client, logger: logger}, nil
}

// Close releases the Redis client.
func (r *Redis) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

// PutProxyState writes one proxy's runtime state. Best-effort: errors are
// logged but never returned to the hot path.
func (r *Redis) PutProxyState(provider string, p *model.Proxy) {
	if r == nil || r.client == nil || p == nil {
		return
	}
	st := ProxyState{
		LatencyMS: p.LatencyMS.Load(),
		Country:   p.Country,
		Alive:     p.Alive.Load(),
		Ts:        time.Now().Unix(),
	}
	data, err := json.Marshal(st)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := r.client.HSet(ctx, proxyKey(provider), p.ID, string(data)).Err(); err != nil {
		r.logger.Debug("redis hset failed", "provider", provider, "proxy", p.ID, "err", err)
	}
}

// RemoveProxy removes one proxy's runtime state.
func (r *Redis) RemoveProxy(provider, id string) {
	if r == nil || r.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := r.client.HDel(ctx, proxyKey(provider), id).Err(); err != nil {
		r.logger.Debug("redis hdel failed", "provider", provider, "err", err)
	}
}

// LoadProxyStates reads all cached proxy states for a provider, restoring
// latency / country so they survive restarts (alive is re-verified by the
// health checker).
func (r *Redis) LoadProxyStates(provider string) (map[string]ProxyState, error) {
	if r == nil || r.client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw, err := r.client.HGetAll(ctx, proxyKey(provider)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]ProxyState, len(raw))
	for id, v := range raw {
		var st ProxyState
		if json.Unmarshal([]byte(v), &st) == nil {
			out[id] = st
		}
	}
	return out, nil
}
