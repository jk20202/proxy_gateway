package auth

import (
	"testing"

	"proxy-pool/internal/config"
)

func testAccts() []config.AccountCfg {
	return []config.AccountCfg{
		{Name: "admin", Password: "adminpw", Token: "admintok", Role: "admin", Enabled: true},
		{Name: "alice", Password: "alicepw", Token: "alicetok", Role: "user", Enabled: true, Groups: []string{"g1"}},
		{Name: "bob", Password: "bobpw", Token: "bobtok", Role: "user", Enabled: false},
	}
}

func TestLoginSuccess(t *testing.T) {
	m := New(testAccts())
	acct, ok := m.Login("admin", "adminpw")
	if !ok {
		t.Fatal("expected successful login")
	}
	if acct.Token != "admintok" {
		t.Fatalf("expected admin token, got %s", acct.Token)
	}
	if !acct.IsAdmin() {
		t.Fatal("admin should have admin role")
	}
}

func TestLoginWrongPassword(t *testing.T) {
	m := New(testAccts())
	if _, ok := m.Login("admin", "wrong"); ok {
		t.Fatal("expected login failure with wrong password")
	}
}

func TestLoginDisabledAccount(t *testing.T) {
	m := New(testAccts())
	if _, ok := m.Login("bob", "bobpw"); ok {
		t.Fatal("expected login failure for disabled account")
	}
}

func TestLoginUnknownName(t *testing.T) {
	m := New(testAccts())
	if _, ok := m.Login("nobody", "x"); ok {
		t.Fatal("expected login failure for unknown name")
	}
}

func TestByToken(t *testing.T) {
	m := New(testAccts())
	acct, ok := m.ByToken("alicetok")
	if !ok {
		t.Fatal("expected token lookup success")
	}
	if acct.Name != "alice" {
		t.Fatalf("expected alice, got %s", acct.Name)
	}
	if acct.CanUseGroup("g1") {
		// allowed
	} else {
		t.Fatal("alice should be allowed g1")
	}
	if acct.CanUseGroup("g2") {
		t.Fatal("alice should NOT be allowed g2")
	}
}

func TestByTokenUnknown(t *testing.T) {
	m := New(testAccts())
	if _, ok := m.ByToken("nope"); ok {
		t.Fatal("expected failure for unknown token")
	}
}

func TestByTokenDisabled(t *testing.T) {
	m := New(testAccts())
	if _, ok := m.ByToken("bobtok"); ok {
		t.Fatal("expected failure for disabled account token")
	}
}

func TestEmpty(t *testing.T) {
	if m := New(nil); !m.Empty() {
		t.Fatal("nil accounts should be empty")
	}
	if m := New(testAccts()); m.Empty() {
		t.Fatal("configured accounts should not be empty")
	}
}

func TestCanUseGroupEmptyMeansAll(t *testing.T) {
	acct := &Account{Name: "x", Role: "user", Groups: nil}
	if !acct.CanUseGroup("anything") {
		t.Fatal("empty group list should allow all groups")
	}
}

func TestAddAccount(t *testing.T) {
	m := New(testAccts())
	err := m.AddAccount(config.AccountCfg{Name: "carol", Password: "cp", Token: "ctok", Role: "user", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := m.Login("carol", "cp"); !ok {
		t.Fatal("expected carol login after add")
	}
}

func TestAddAccountValidation(t *testing.T) {
	m := New(nil)
	if err := m.AddAccount(config.AccountCfg{Name: "", Token: "t"}); err == nil {
		t.Fatal("expected error for empty name")
	}
	// empty token is allowed: a random token is generated
	if err := m.AddAccount(config.AccountCfg{Name: "a", Role: "user", Enabled: true}); err != nil {
		t.Fatalf("expected empty token to auto-generate: %v", err)
	}
	if acct, ok := m.ByToken("a"); ok && acct.Name == "a" {
		t.Fatal("generated token should not be the account name")
	}
	if err := m.AddAccount(config.AccountCfg{Name: "b", Token: "t", Role: "superuser"}); err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestAddAccountReplacesToken(t *testing.T) {
	m := New(testAccts())
	if err := m.AddAccount(config.AccountCfg{Name: "alice", Password: "alicepw", Token: "newtok", Role: "user", Enabled: true, Groups: []string{"g1"}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// old token should no longer resolve
	if _, ok := m.ByToken("alicetok"); ok {
		t.Fatal("old token should be invalid after replacement")
	}
	if _, ok := m.ByToken("newtok"); !ok {
		t.Fatal("new token should resolve")
	}
}

func TestRemoveAccount(t *testing.T) {
	m := New(testAccts())
	if err := m.RemoveAccount("alice"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := m.ByToken("alicetok"); ok {
		t.Fatal("removed account token should be invalid")
	}
	if err := m.RemoveAccount("alice"); err == nil {
		t.Fatal("expected error removing unknown account")
	}
}

func TestUpdateAccount(t *testing.T) {
	m := New(testAccts())
	// partial update: change role, keep existing token
	err := m.UpdateAccount("alice", config.AccountCfg{Name: "alice", Role: "admin", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := m.ByToken("alicetok"); !ok {
		t.Fatal("unchanged token should still resolve")
	}
	if acct, _ := m.ByToken("alicetok"); acct.Role != "admin" {
		t.Fatalf("expected admin role, got %s", acct.Role)
	}
	// explicit new token replaces the mapping
	err = m.UpdateAccount("alice", config.AccountCfg{Name: "alice", Token: "alice2", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := m.ByToken("alicetok"); ok {
		t.Fatal("old token should be invalid after update")
	}
	if _, ok := m.ByToken("alice2"); !ok {
		t.Fatal("new token should resolve")
	}
	// unknown account
	if err := m.UpdateAccount("nobody", config.AccountCfg{Name: "nobody", Enabled: true}); err == nil {
		t.Fatal("expected error updating unknown account")
	}
	// invalid role
	if err := m.UpdateAccount("alice", config.AccountCfg{Name: "alice", Role: "root", Enabled: true}); err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestListOmitsPasswords(t *testing.T) {
	m := New(testAccts())
	for _, a := range m.List() {
		if a.Password != "" {
			t.Fatalf("List should omit password for %s", a.Name)
		}
	}
}
