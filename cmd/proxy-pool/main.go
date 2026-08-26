package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"

	"proxy-pool/internal/alert"
	"proxy-pool/internal/auth"
	"proxy-pool/internal/config"
	"proxy-pool/internal/gateway"
	"proxy-pool/internal/geo"
	"proxy-pool/internal/health"
	"proxy-pool/internal/model"
	"proxy-pool/internal/persist"
	"proxy-pool/internal/pool"
	"proxy-pool/internal/server"
	"proxy-pool/internal/store"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	alertsFile := flag.String("alerts-file", "alerts.json", "path to alert config persistence file (empty to disable)")
	groupsFile := flag.String("groups-file", "groups.json", "path to group config persistence file (empty to disable)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Optional persistent storage: MySQL holds low-frequency settings (accounts /
	// groups / provider configs) so runtime edits survive restarts; Redis caches
	// high-frequency proxy state (latency / country / alive).
	// Connections are retried for a short window so the process tolerates a
	// database that is still starting up (e.g. first run of docker compose).
	db, err := openMySQLWithRetry(cfg.Storage.MySQL, logger, 10)
	if err != nil {
		logger.Error("failed to open mysql storage", "err", err)
		os.Exit(1)
	}
	if db != nil {
		defer db.Close()
		logger.Info("mysql storage enabled")
		// Provider configs stored in MySQL take precedence over config.yaml so
		// runtime edits survive restarts. Seed the database on first start.
		if fromDB, err := db.LoadProviders(); err != nil {
			logger.Error("failed to load providers from mysql", "err", err)
		} else if len(fromDB) > 0 {
			cfg.Providers = fromDB
		}
	}
	redis, err := openRedisWithRetry(cfg.Storage.Redis, logger, 10)
	if err != nil {
		logger.Error("failed to open redis storage", "err", err)
		os.Exit(1)
	}
	if redis != nil {
		defer redis.Close()
		logger.Info("redis storage enabled")
	}

	mgr, err := pool.NewManager(cfg, logger)
	if err != nil {
		logger.Error("failed to init manager", "err", err)
		os.Exit(1)
	}
	if db != nil {
		mgr.AttachMySQL(db)
		if err := mgr.SyncProvidersToDB(); err != nil {
			logger.Error("failed to seed providers into mysql", "err", err)
		}
	}
	if redis != nil {
		mgr.AttachRedis(redis)
		mgr.Pool().SetStateSink(func(pr *model.Proxy, removed bool) {
			if removed {
				redis.RemoveProxy(pr.Provider, pr.ID)
			} else {
				redis.PutProxyState(pr.Provider, pr)
			}
		})
	}
	if cfg.Geo.Enabled {
		gc := geo.New(cfg.Geo.URL)
		mgr.SetGeo(gc)
		go mgr.EnrichCountries(ctx)
		logger.Info("geo enrichment enabled", "url", cfg.Geo.URL, "interval_s", cfg.Geo.IntervalSecs)
	}
	mgr.Pool().SetGroups(cfg.Groups)

	dispatcher := alert.NewDispatcher(cfg.Alerts, logger)
	if *alertsFile != "" {
		dispatcher.SetFile(*alertsFile)
		if err := dispatcher.LoadFile(); err != nil {
			logger.Error("failed to load alert config file", "file", *alertsFile, "err", err)
		}
	}
	mgr.AlertEmit = func(eventType, provider, message string, data map[string]any) {
		dispatcher.Emit(alert.Event{
			Type:     eventType,
			Provider: provider,
			Message:  message,
			Data:     data,
		})
	}
	monitor := alert.NewProviderMonitor(mgr, cfg.Alerts, dispatcher, logger)
	go monitor.Run(ctx)

	mgr.LoadInitial(ctx)
	if redis != nil {
		// Replay cached latency / country from Redis so health data survives
		// restarts before the checker re-verifies aliveness.
		mgr.RestoreFromRedis()
	}

	checker := health.NewChecker(cfg.HealthCheck, mgr.Pool(), logger)
	go checker.Run(ctx)

	go mgr.RefreshLoop(ctx)

	srv := server.New(*cfg, mgr, checker, logger)
	srv.AttachAlerts(dispatcher)
	srv.AttachGroups(cfg.Groups)
	if db != nil {
		// Groups stored in MySQL take precedence over config.yaml so runtime
		// edits survive restarts; AttachMySQL seeds the table on first start.
		srv.AttachMySQL(db)
	}
	if *groupsFile != "" {
		if err := srv.SetGroupsFile(*groupsFile); err != nil {
			logger.Error("failed to load group config file", "file", *groupsFile, "err", err)
		}
	}
	{
		// Accounts stored in MySQL take precedence over config.yaml. When the
		// database is empty the configured accounts are seeded into it.
		accounts := cfg.Accounts
		if db != nil {
			if fromDB, err := db.LoadAccounts(); err != nil {
				logger.Error("failed to load accounts from mysql", "err", err)
			} else if len(fromDB) > 0 {
				accounts = fromDB
			}
		}
		if len(accounts) > 0 {
			am := auth.New(accounts)
			if db != nil {
				am.AttachMySQL(db)
				if err := am.SyncAll(); err != nil {
					logger.Error("failed to seed accounts into mysql", "err", err)
				}
			}
			srv.AttachAuth(am)
		}
	}
	usageStore, err := store.Open(cfg.DB, logger)
	if err != nil {
		logger.Error("failed to open usage store", "err", err)
		os.Exit(1)
	}
	if usageStore != nil {
		srv.AttachUsage(usageStore)
		defer usageStore.Close()
	}
	handler := srv.Handler()

	// HTTP proxy gateway: forward traffic through a live proxy from the group
	// matched by `username:password` credentials configured per group.
	//
	// When gateway_listen is empty or equals the console listen address, the
	// gateway is served on the same port as the admin console (it is wired into
	// the fasthttp handler as a form of standard forward-proxy / CONNECT
	// handling). Otherwise it runs as a standalone listener on its own port,
	// preserving the earlier two-port layout.
	gwMerged := cfg.Server.GatewayListen == "" || cfg.Server.GatewayListen == cfg.Server.Listen
	attachGateway := func() error {
		gw := gateway.New(mgr.Pool(), func() []config.GroupCfg {
			return srv.GroupList()
		}, logger)
		return srv.AttachGateway(gw)
	}
	if gwMerged {
		// Merged mode: the gateway is always created and served on the console
		// port so `curl -x user:pass@host:listen` works on the same address.
		if err := attachGateway(); err != nil {
			logger.Error("failed to attach merged gateway", "err", err)
			cancel()
		} else {
			logger.Info("proxy gateway merged onto console port", "listen", cfg.Server.Listen)
		}
	} else {
		gw := gateway.New(mgr.Pool(), func() []config.GroupCfg {
			return srv.GroupList()
		}, logger)
		if err := srv.AttachGateway(gw); err != nil {
			logger.Error("failed to attach standalone gateway", "err", err)
			cancel()
		}
		go func() {
			if err := gw.Serve(ctx, cfg.Server.GatewayListen); err != nil {
				logger.Error("proxy gateway stopped", "err", err)
				cancel()
			}
		}()
	}

	srvCfg := &fasthttp.Server{
		Handler:            handler,
		ReadTimeout:        time.Duration(cfg.Server.ReadTimeout) * time.Millisecond,
		WriteTimeout:       time.Duration(cfg.Server.WriteTimeout) * time.Millisecond,
		MaxConnsPerIP:      0,
		MaxRequestBodySize: 1024 * 1024,
		Concurrency:        cfg.Server.MaxWorkers,
	}

	go func() {
		logger.Info("proxy pool started", "listen", cfg.Server.Listen)
		if err := srvCfg.ListenAndServe(cfg.Server.Listen); err != nil {
			logger.Error("server stopped", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	_ = srvCfg.Shutdown()
}

// openMySQLWithRetry tries to open MySQL, retrying up to n attempts with a 3s
// pause between them. An unconfigured storage returns (nil, nil) immediately.
func openMySQLWithRetry(cfg config.MySQLConfig, logger *slog.Logger, n int) (*persist.MySQL, error) {
	var lastErr error
	for i := 0; i < n; i++ {
		db, err := persist.OpenMySQL(cfg, logger)
		if err == nil {
			return db, nil
		}
		lastErr = err
		if db != nil {
			db.Close()
		}
		logger.Warn("mysql not ready, retrying", "attempt", i+1, "err", err)
		time.Sleep(3 * time.Second)
	}
	return nil, lastErr
}

// openRedisWithRetry tries to open Redis, retrying up to n attempts with a 3s
// pause between them. An unconfigured storage returns (nil, nil) immediately.
func openRedisWithRetry(cfg config.RedisConfig, logger *slog.Logger, n int) (*persist.Redis, error) {
	var lastErr error
	for i := 0; i < n; i++ {
		r, err := persist.OpenRedis(cfg, logger)
		if err == nil {
			return r, nil
		}
		lastErr = err
		if r != nil {
			r.Close()
		}
		logger.Warn("redis not ready, retrying", "attempt", i+1, "err", err)
		time.Sleep(3 * time.Second)
	}
	return nil, lastErr
}
