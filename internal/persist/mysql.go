// Package persist provides optional persistent storage: MySQL for low-frequency
// settings (accounts / groups / provider configs) and Redis for high-frequency
// proxy runtime state (latency / country / alive).
package persist

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"proxy-pool/internal/config"
)

// MySQL wraps the optional MySQL connection used to persist settings that must
// survive restarts: accounts, scheduling groups and provider configs.
type MySQL struct {
	db     *sql.DB
	logger *slog.Logger
}

// OpenMySQL connects to MySQL and ensures the schema exists. Returns nil when
// storage is not configured.
func OpenMySQL(cfg config.MySQLConfig, logger *slog.Logger) (*MySQL, error) {
	dsn := cfg.DSN
	if dsn == "" {
		if cfg.Addr == "" {
			return nil, nil
		}
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&charset=utf8mb4",
			cfg.User, cfg.Pass, cfg.Addr, cfg.DB)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	m := &MySQL{db: db, logger: logger}
	if err := m.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql migrate: %w", err)
	}
	return m, nil
}

// Close releases the connection pool.
func (m *MySQL) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *MySQL) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			name VARCHAR(128) PRIMARY KEY,
			password VARCHAR(255) NOT NULL DEFAULT '',
			token VARCHAR(64) NOT NULL DEFAULT '',
			role VARCHAR(16) NOT NULL DEFAULT 'user',
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			groups_json TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS groups (
			name VARCHAR(128) PRIMARY KEY,
			cfg_json MEDIUMTEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS providers (
			name VARCHAR(128) PRIMARY KEY,
			cfg_json MEDIUMTEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}
	for _, s := range stmts {
		if _, err := m.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}
