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

	mgr, err := pool.NewManager(cfg, logger)
	if err != nil {
		logger.Error("failed to init manager", "err", err)
		os.Exit(1)
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

	checker := health.NewChecker(cfg.HealthCheck, mgr.Pool(), logger)
	go checker.Run(ctx)

	go mgr.RefreshLoop(ctx)

	srv := server.New(*cfg, mgr, checker, logger)
	srv.AttachAlerts(dispatcher)
	srv.AttachGroups(cfg.Groups)
	if *groupsFile != "" {
		if err := srv.SetGroupsFile(*groupsFile); err != nil {
			logger.Error("failed to load group config file", "file", *groupsFile, "err", err)
		}
	}
	if len(cfg.Accounts) > 0 {
		srv.AttachAuth(auth.New(cfg.Accounts))
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
	if cfg.Server.GatewayListen != "" {
		gw := gateway.New(mgr.Pool(), func() []config.GroupCfg {
			return srv.GroupList()
		}, logger)
		srv.AttachGateway(gw)
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
