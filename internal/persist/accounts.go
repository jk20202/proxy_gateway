package persist

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"proxy-pool/internal/config"
)

// LoadAccounts reads all accounts from MySQL. Returns nil when no rows exist.
func (m *MySQL) LoadAccounts() ([]config.AccountCfg, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query(`SELECT name, password, token, role, enabled, groups_json FROM accounts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []config.AccountCfg
	for rows.Next() {
		var a config.AccountCfg
		var groupsJSON string
		if err := rows.Scan(&a.Name, &a.Password, &a.Token, &a.Role, &a.Enabled, &groupsJSON); err != nil {
			return nil, err
		}
		if groupsJSON != "" {
			_ = json.Unmarshal([]byte(groupsJSON), &a.Groups)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SaveAccount upserts one account.
func (m *MySQL) SaveAccount(a config.AccountCfg) error {
	if m == nil || m.db == nil {
		return nil
	}
	groupsJSON, err := json.Marshal(a.Groups)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = m.db.ExecContext(ctx, `INSERT INTO accounts (name, password, token, role, enabled, groups_json)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE password=VALUES(password), token=VALUES(token),
			role=VALUES(role), enabled=VALUES(enabled), groups_json=VALUES(groups_json)`,
		a.Name, a.Password, a.Token, a.Role, a.Enabled, string(groupsJSON))
	return err
}

// DeleteAccount removes an account by name.
func (m *MySQL) DeleteAccount(name string) error {
	if m == nil || m.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.db.ExecContext(ctx, `DELETE FROM accounts WHERE name = ?`, name)
	return err
}

// ReplaceAccounts atomically replaces the whole account table with the given
// list. Used at startup to seed the database from config.yaml only when the
// table is empty.
func (m *MySQL) ReplaceAccounts(accts []config.AccountCfg) error {
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
	for _, a := range accts {
		groupsJSON, _ := json.Marshal(a.Groups)
		if _, err := tx.ExecContext(ctx, `INSERT INTO accounts (name, password, token, role, enabled, groups_json)
			VALUES (?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE password=VALUES(password), token=VALUES(token),
				role=VALUES(role), enabled=VALUES(enabled), groups_json=VALUES(groups_json)`,
			a.Name, a.Password, a.Token, a.Role, a.Enabled, string(groupsJSON)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var _ = sql.ErrNoRows
