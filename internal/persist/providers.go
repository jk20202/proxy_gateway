package persist

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"proxy-pool/internal/config"
)

// LoadProviders reads all provider configs from MySQL. Returns nil when empty.
func (m *MySQL) LoadProviders() ([]config.ProviderCfg, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query(`SELECT cfg_json FROM providers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []config.ProviderCfg
	for rows.Next() {
		var cfgJSON string
		if err := rows.Scan(&cfgJSON); err != nil {
			return nil, err
		}
		var p config.ProviderCfg
		if err := json.Unmarshal([]byte(cfgJSON), &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SaveProvider upserts one provider config.
func (m *MySQL) SaveProvider(p config.ProviderCfg) error {
	if m == nil || m.db == nil {
		return nil
	}
	cfgJSON, err := json.Marshal(p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = m.db.ExecContext(ctx, `INSERT INTO providers (name, cfg_json) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE cfg_json=VALUES(cfg_json)`, p.Name, string(cfgJSON))
	return err
}

// DeleteProvider removes a provider config by name.
func (m *MySQL) DeleteProvider(name string) error {
	if m == nil || m.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.db.ExecContext(ctx, `DELETE FROM providers WHERE name = ?`, name)
	return err
}

// ReplaceProviders atomically replaces the whole provider table. Used at
// startup to seed the database from config.yaml only when the table is empty.
func (m *MySQL) ReplaceProviders(ps []config.ProviderCfg) error {
	if m == nil || m.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range ps {
		cfgJSON, _ := json.Marshal(p)
		if _, err := tx.ExecContext(ctx, `INSERT INTO providers (name, cfg_json) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE cfg_json=VALUES(cfg_json)`, p.Name, string(cfgJSON)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var _ = sql.ErrNoRows
