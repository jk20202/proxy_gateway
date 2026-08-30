package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"strings"
	"sync"

	"proxy-pool/internal/config"
	"proxy-pool/internal/persist"
)

// NewToken generates a random API token for accounts that do not provide one.
func NewToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Account is a runtime view of an internal user.
type Account struct {
	Name          string
	Password      string
	Token         string
	Role          string
	Enabled       bool
	Groups        []string
	Subscriptions []string
}

// IsAdmin reports whether the account has the admin role.
func (a *Account) IsAdmin() bool {
	return a.Role == "admin"
}

// CanUseGroup reports whether the account may consume proxies from the group.
// Empty Groups means all groups are allowed.
func (a *Account) CanUseGroup(group string) bool {
	if len(a.Groups) == 0 {
		return true
	}
	for _, g := range a.Groups {
		if g == group {
			return true
		}
	}
	return false
}

// IsSubscribed reports whether the account has subscribed to the given
// provider name. A subscription grants a non-owner access to a shared
// provider's proxies, stats and group inclusion.
func (a *Account) IsSubscribed(name string) bool {
	if a == nil {
		return false
	}
	for _, s := range a.Subscriptions {
		if s == name {
			return true
		}
	}
	return false
}

// Manager validates web logins and API tokens against configured accounts.
type Manager struct {
	mu      sync.RWMutex
	byName  map[string]*Account
	byToken map[string]*Account
	persist *persist.MySQL
}

// AttachMySQL wires optional MySQL persistence so account mutations survive
// restarts. Pass nil to keep accounts in-memory only.
func (m *Manager) AttachMySQL(p *persist.MySQL) {
	m.mu.Lock()
	m.persist = p
	m.mu.Unlock()
}

// SyncAll seeds MySQL with the current account set. Called once at startup so
// config.yaml accounts appear in the database even when the table was empty.
func (m *Manager) SyncAll() error {
	m.mu.RLock()
	accts := make([]config.AccountCfg, 0, len(m.byName))
	for _, a := range m.byName {
		accts = append(accts, config.AccountCfg{
			Name: a.Name, Password: a.Password, Token: a.Token,
			Role: a.Role, Enabled: a.Enabled, Groups: a.Groups, Subscriptions: a.Subscriptions,
		})
	}
	pers := m.persist
	m.mu.RUnlock()
	if pers == nil {
		return nil
	}
	return pers.ReplaceAccounts(accts)
}

// New builds an account manager from the given account configs.
func New(cfgs []config.AccountCfg) *Manager {
	m := &Manager{
		byName:  make(map[string]*Account, len(cfgs)),
		byToken: make(map[string]*Account, len(cfgs)),
	}
	for _, c := range cfgs {
		acct := &Account{
			Name:          c.Name,
			Password:      c.Password,
			Token:         c.Token,
			Role:          c.Role,
			Enabled:       c.Enabled,
			Groups:        c.Groups,
			Subscriptions: c.Subscriptions,
		}
		if acct.Role == "" {
			acct.Role = "user"
		}
		m.byName[acct.Name] = acct
		if acct.Token != "" {
			m.byToken[acct.Token] = acct
		}
	}
	return m
}

// Empty reports whether no accounts are configured (auth disabled).
func (m *Manager) Empty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byName) == 0
}

// Login authenticates by name+password and returns the account's API token.
func (m *Manager) Login(name, password string) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	acct := m.byName[name]
	if acct == nil || !acct.Enabled {
		return nil, false
	}
	if subtle.ConstantTimeCompare([]byte(acct.Password), []byte(password)) != 1 {
		return nil, false
	}
	return acct, true
}

// ByToken resolves an account from an API token.
func (m *Manager) ByToken(token string) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	acct := m.byToken[token]
	if acct == nil || !acct.Enabled {
		return nil, false
	}
	return acct, true
}

// List returns all accounts (password omitted).
func (m *Manager) List() []config.AccountCfg {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]config.AccountCfg, 0, len(m.byName))
	for _, a := range m.byName {
		out = append(out, config.AccountCfg{
			Name: a.Name, Password: "", Token: a.Token, Role: a.Role, Enabled: a.Enabled, Groups: a.Groups, Subscriptions: a.Subscriptions,
		})
	}
	return out
}

// AddAccount inserts or replaces an account dynamically. When the token is
// empty a random one is generated so callers never control the token.
func (m *Manager) AddAccount(c config.AccountCfg) error {
	if strings.TrimSpace(c.Name) == "" {
		return &ConfigError{"name is required"}
	}
	if c.Role == "" {
		c.Role = "user"
	}
	if c.Role != "admin" && c.Role != "user" {
		return &ConfigError{"role must be admin or user"}
	}
	if strings.TrimSpace(c.Token) == "" {
		c.Token = NewToken()
	}
	acct := &Account{
		Name: c.Name, Password: c.Password, Token: c.Token, Role: c.Role, Enabled: c.Enabled, Groups: c.Groups, Subscriptions: c.Subscriptions,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// remove any previous token mapping for this account
	if old := m.byName[c.Name]; old != nil && old.Token != "" {
		delete(m.byToken, old.Token)
	}
	m.byToken[c.Token] = acct
	m.byName[c.Name] = acct
	if m.persist != nil {
		if err := m.persist.SaveAccount(config.AccountCfg{
			Name: acct.Name, Password: acct.Password, Token: acct.Token,
			Role: acct.Role, Enabled: acct.Enabled, Groups: acct.Groups, Subscriptions: acct.Subscriptions,
		}); err != nil {
			return err
		}
	}
	return nil
}

// UpdateSubscriptions replaces the account's subscription list (provider names
// the account has subscribed to from the shared market). Pass a nil slice to
// clear all subscriptions.
func (m *Manager) UpdateSubscriptions(name string, subs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	acct := m.byName[name]
	if acct == nil {
		return &ConfigError{"account not found"}
	}
	acct.Subscriptions = subs
	if m.persist != nil {
		return m.persist.SaveAccount(config.AccountCfg{
			Name: acct.Name, Password: acct.Password, Token: acct.Token,
			Role: acct.Role, Enabled: acct.Enabled, Groups: acct.Groups, Subscriptions: acct.Subscriptions,
		})
	}
	return nil
}

// UpdateAccount applies a partial update to an existing account. The token is
// regenerated automatically when an empty token is provided. Password is kept
// unchanged when empty, and Groups is kept unchanged when nil (empty slice
// clears the group restriction). When c.Name differs from name the account is
// renamed, so callers can edit the account name in-place.
func (m *Manager) UpdateAccount(name string, c config.AccountCfg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	acct := m.byName[name]
	if acct == nil {
		return &ConfigError{"account not found"}
	}
	if c.Role != "" {
		if c.Role != "admin" && c.Role != "user" {
			return &ConfigError{"role must be admin or user"}
		}
		acct.Role = c.Role
	}
	if c.Password != "" {
		acct.Password = c.Password
	}
	if c.Groups != nil {
		acct.Groups = c.Groups
	}
	if c.Subscriptions != nil {
		acct.Subscriptions = c.Subscriptions
	}
	acct.Enabled = c.Enabled
	if c.Token != "" {
		if old := acct.Token; old != "" {
			delete(m.byToken, old)
		}
		acct.Token = c.Token
		m.byToken[c.Token] = acct
	}
	// Rename support: when the request carries a different non-empty name,
	// update the account's name and all index mappings.
	newName := strings.TrimSpace(c.Name)
	if newName != "" && newName != name {
		if m.byName[newName] != nil {
			return &ConfigError{"account name already exists"}
		}
		delete(m.byName, name)
		acct.Name = newName
		m.byName[newName] = acct
	}
	if m.persist != nil {
		// Persist under the (possibly renamed) key. For a rename the new row
		// is upserted and the old name is removed so no stale row remains.
		if err := m.persist.SaveAccount(config.AccountCfg{
			Name: acct.Name, Password: acct.Password, Token: acct.Token,
			Role: acct.Role, Enabled: acct.Enabled, Groups: acct.Groups, Subscriptions: acct.Subscriptions,
		}); err != nil {
			return err
		}
		if newName != "" && newName != name {
			return m.persist.DeleteAccount(name)
		}
	}
	return nil
}

// RemoveAccount deletes an account.
func (m *Manager) RemoveAccount(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	acct := m.byName[name]
	if acct == nil {
		return &ConfigError{"account not found"}
	}
	if acct.Token != "" {
		delete(m.byToken, acct.Token)
	}
	delete(m.byName, name)
	if m.persist != nil {
		return m.persist.DeleteAccount(name)
	}
	return nil
}

// ConfigError is a validation error returned by the account manager.
type ConfigError struct{ msg string }

func (e *ConfigError) Error() string { return e.msg }
