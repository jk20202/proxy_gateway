// Package persist provides file-backed persistent storage for the low-frequency
// settings that must survive restarts: accounts, scheduling groups, provider
// configs and system settings. Each entity is stored as an indented JSON file
// inside a single data directory, which keeps the service a single self-
// contained binary with no external database dependency.
package persist

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"proxy-pool/internal/config"
)

// File persists configuration entities to JSON files in a data directory.
// It mirrors the method surface that was previously provided by MySQL so that
// the pool manager, auth manager and server can swap the backing store without
// structural changes. Reads tolerate a missing file (treated as empty); writes
// are atomic (temp file + rename) and serialized by a mutex.
type File struct {
	dir    string
	logger *slog.Logger
	mu     sync.Mutex
}

// OpenFile prepares the data directory for JSON-file persistence. An empty dir
// returns (nil, nil) to disable persistence (in-memory only).
func OpenFile(dir string, logger *slog.Logger) (*File, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &File{dir: dir, logger: logger}, nil
}

// Close is a no-op kept for interface symmetry with the previous database store.
func (f *File) Close() error { return nil }

func (f *File) path(name string) string { return filepath.Join(f.dir, name) }

// read unmarshals a JSON file into v. A missing file yields the errNotExist
// sentinel so callers can treat it as empty.
func (f *File) read(name string, v any) error {
	data, err := os.ReadFile(f.path(name))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, v)
}

// write marshals v and atomically replaces the target file.
func (f *File) write(name string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path(name + ".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path(name)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// ---- Accounts ----

func (f *File) loadAccountsLocked() ([]config.AccountCfg, error) {
	var out []config.AccountCfg
	if err := f.read("accounts.json", &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// LoadAccounts reads all accounts from the file. Returns nil when none exist.
func (f *File) LoadAccounts() ([]config.AccountCfg, error) {
	if f == nil {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadAccountsLocked()
}

// SaveAccount upserts one account.
func (f *File) SaveAccount(a config.AccountCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	accts, err := f.loadAccountsLocked()
	if err != nil {
		return err
	}
	replaced := false
	for i := range accts {
		if accts[i].Name == a.Name {
			accts[i] = a
			replaced = true
			break
		}
	}
	if !replaced {
		accts = append(accts, a)
	}
	return f.write("accounts.json", accts)
}

// DeleteAccount removes an account by name.
func (f *File) DeleteAccount(name string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	accts, err := f.loadAccountsLocked()
	if err != nil {
		return err
	}
	out := accts[:0]
	for _, a := range accts {
		if a.Name != name {
			out = append(out, a)
		}
	}
	return f.write("accounts.json", out)
}

// ReplaceAccounts atomically replaces the whole account list.
func (f *File) ReplaceAccounts(accts []config.AccountCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.write("accounts.json", accts)
}

// ---- Providers ----

func (f *File) loadProvidersLocked() ([]config.ProviderCfg, error) {
	var out []config.ProviderCfg
	if err := f.read("providers.json", &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// LoadProviders reads all provider configs from the file.
func (f *File) LoadProviders() ([]config.ProviderCfg, error) {
	if f == nil {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadProvidersLocked()
}

// SaveProvider upserts one provider config.
func (f *File) SaveProvider(p config.ProviderCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ps, err := f.loadProvidersLocked()
	if err != nil {
		return err
	}
	replaced := false
	for i := range ps {
		if ps[i].Name == p.Name {
			ps[i] = p
			replaced = true
			break
		}
	}
	if !replaced {
		ps = append(ps, p)
	}
	return f.write("providers.json", ps)
}

// DeleteProvider removes a provider config by name.
func (f *File) DeleteProvider(name string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ps, err := f.loadProvidersLocked()
	if err != nil {
		return err
	}
	out := ps[:0]
	for _, p := range ps {
		if p.Name != name {
			out = append(out, p)
		}
	}
	return f.write("providers.json", out)
}

// ReplaceProviders atomically replaces the whole provider list.
func (f *File) ReplaceProviders(ps []config.ProviderCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.write("providers.json", ps)
}

// ---- Groups ----

func (f *File) loadGroupsLocked() ([]config.GroupCfg, error) {
	var out []config.GroupCfg
	if err := f.read("groups.json", &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// LoadGroups reads all scheduling groups from the file.
func (f *File) LoadGroups() ([]config.GroupCfg, error) {
	if f == nil {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadGroupsLocked()
}

// SaveGroup upserts one group.
func (f *File) SaveGroup(g config.GroupCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	gs, err := f.loadGroupsLocked()
	if err != nil {
		return err
	}
	replaced := false
	for i := range gs {
		if gs[i].Name == g.Name {
			gs[i] = g
			replaced = true
			break
		}
	}
	if !replaced {
		gs = append(gs, g)
	}
	return f.write("groups.json", gs)
}

// DeleteGroup removes a group by name.
func (f *File) DeleteGroup(name string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	gs, err := f.loadGroupsLocked()
	if err != nil {
		return err
	}
	out := gs[:0]
	for _, g := range gs {
		if g.Name != name {
			out = append(out, g)
		}
	}
	return f.write("groups.json", out)
}

// ReplaceGroups atomically replaces the whole group list.
func (f *File) ReplaceGroups(gs []config.GroupCfg) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.write("groups.json", gs)
}
