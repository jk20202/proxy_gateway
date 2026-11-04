package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"

	"proxy-pool/internal/alert"
	"proxy-pool/internal/auth"
	"proxy-pool/internal/config"
	"proxy-pool/internal/gateway"
	"proxy-pool/internal/geo"
	"proxy-pool/internal/health"
	"proxy-pool/internal/persist"
	"proxy-pool/internal/pool"
	"proxy-pool/internal/server"
	"proxy-pool/internal/store"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	alertsFile := flag.String("alerts-file", "alerts.json", "path to alert config persistence file (empty to disable)")
	dataDir := flag.String("data-dir", "", "directory for JSON persistence (empty = use config data_dir)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// File-backed persistence: accounts / providers / groups / settings live as
	// JSON files inside cfg.DataDir so the service is a single self-contained
	// binary with no external database dependency.
	fs, err := persist.OpenFile(cfg.DataDir, logger)
	if err != nil {
		logger.Error("failed to open file store", "dir", cfg.DataDir, "err", err)
		os.Exit(1)
	}
	if fs != nil {
		logger.Info("file persistence enabled", "dir", cfg.DataDir)
		// Provider configs stored on disk take precedence over config.yaml so
		// runtime edits survive restarts. Seed the file on first start.
		if fromFile, err := fs.LoadProviders(); err != nil {
			logger.Error("failed to load providers from file", "err", err)
		} else if len(fromFile) > 0 {
			cfg.Providers = fromFile
		}
	}

	mgr, err := pool.NewManager(cfg, logger)
	if err != nil {
		logger.Error("failed to init manager", "err", err)
		os.Exit(1)
	}
	if fs != nil {
		mgr.AttachStore(fs)
		if err := mgr.SyncProvidersToStore(); err != nil {
			logger.Error("failed to seed providers into file", "err", err)
		}
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
	if fs != nil {
		// Groups stored on disk take precedence over config.yaml so runtime
		// edits survive restarts; AttachStore seeds the file on first start.
		srv.AttachStore(fs)
	}
	{
		// Accounts stored on disk take precedence over config.yaml. When the
		// file is empty the configured accounts are seeded into it.
		accounts := cfg.Accounts
		if fs != nil {
			if fromFile, err := fs.LoadAccounts(); err != nil {
				logger.Error("failed to load accounts from file", "err", err)
			} else if len(fromFile) > 0 {
				accounts = fromFile
			}
		}
		if len(accounts) > 0 {
			am := auth.New(accounts)
			if fs != nil {
				am.AttachStore(fs)
				if err := am.SyncAll(); err != nil {
					logger.Error("failed to seed accounts into file", "err", err)
				}
			}
			srv.AttachAuth(am)
		}
	}
	// System settings (registration switch, site name, theme, health-check
	// tuning) persisted to settings.json and hot-applied to the checker.
	{
		settingsPath := ""
		if cfg.DataDir != "" {
			settingsPath = filepath.Join(cfg.DataDir, "settings.json")
		}
		st, err := server.NewSettingsStore(settingsPath, cfg.Settings)
		if err != nil {
			logger.Error("failed to load settings store", "err", err)
			os.Exit(1)
		}
		st.SetOnApply(func(s config.Settings) {
			checker.SetConfig(s.HealthCheck)
			logger.Info("settings applied", "registration_enabled", s.RegistrationEnabled, "theme", s.ThemeDefault)
		})
		srv.AttachSettings(st)
		// Apply the persisted health settings immediately on boot.
		checker.SetConfig(st.Get().HealthCheck)
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
	gwMerged := cfg.Server.GatewayListen == "" || cfg.Server.GatewayListen == cfg.Server.Listen
	attachGateway := func() error {
		gw := gateway.New(mgr.Pool(), func() []config.GroupCfg {
			return srv.GroupList()
		}, logger)
		return srv.AttachGateway(gw)
	}
	if gwMerged {
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
