package persist

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"proxy-pool/internal/config"
)

// LoadGroups reads all scheduling groups from MySQL. Returns nil when empty.
func (m *MySQL) LoadGroups() ([]config.GroupCfg, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query(`SELECT cfg_json FROM groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []config.GroupCfg
	for rows.Next() {
		var cfgJSON string
		if err := rows.Scan(&cfgJSON); err != nil {
			return nil, err
		}
		var g config.GroupCfg
		if err := json.Unmarshal([]byte(cfgJSON), &g); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// SaveGroup upserts one group.
func (m *MySQL) SaveGroup(g config.GroupCfg) error {
	if m == nil || m.db == nil {
		return nil
	}
	cfgJSON, err := json.Marshal(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = m.db.ExecContext(ctx, `INSERT INTO groups (name, cfg_json) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE cfg_json=VALUES(cfg_json)`, g.Name, string(cfgJSON))
	return err
}

// DeleteGroup removes a group by name.
func (m *MySQL) DeleteGroup(name string) error {
	if m == nil || m.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.db.ExecContext(ctx, `DELETE FROM groups WHERE name = ?`, name)
	return err
}

// ReplaceGroups atomically replaces the whole group table. Used at startup to
// seed the database from config.yaml only when the table is empty.
func (m *MySQL) ReplaceGroups(gs []config.GroupCfg) error {
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
	for _, g := range gs {
		cfgJSON, _ := json.Marshal(g)
		if _, err := tx.ExecContext(ctx, `INSERT INTO groups (name, cfg_json) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE cfg_json=VALUES(cfg_json)`, g.Name, string(cfgJSON)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var _ = sql.ErrNoRows
