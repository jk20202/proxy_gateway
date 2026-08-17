package store

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"proxy-pool/internal/config"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	s, err := Open(config.DBConfig{Path: path, BatchSize: 50, FlushIntervalMs: 50}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s, path
}

func TestRecordAndCount(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	for range 10 {
		s.Record(Call{Account: "alice", Group: "g1", ProxyID: "p1", Addr: "1.2.3.4:80", Kind: "proxy", OK: true})
	}
	for range 5 {
		s.Record(Call{Account: "bob", Group: "g2", ProxyID: "p2", Addr: "5.6.7.8:80", Kind: "proxy", OK: true})
	}
	// wait for async flush
	deadline := time.Now().Add(3 * time.Second)
	for {
		n, err := s.Count("", 0, 0)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 15 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 15 records, got %d", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if n, _ := s.Count("alice", 0, 0); n != 10 {
		t.Fatalf("expected 10 alice records, got %d", n)
	}
}

func TestSummary(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	s.Record(Call{Account: "alice", OK: true})
	s.Record(Call{Account: "alice", OK: true})
	s.Record(Call{Account: "bob", OK: false})

	deadline := time.Now().Add(3 * time.Second)
	var sum []AccountSummary
	for {
		var err error
		sum, err = s.Summary(0, 0)
		if err != nil {
			t.Fatalf("summary: %v", err)
		}
		if len(sum) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 2 accounts, got %d", len(sum))
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, a := range sum {
		switch a.Account {
		case "alice":
			if a.Total != 2 || a.OK != 2 {
				t.Fatalf("unexpected alice summary: %+v", a)
			}
		case "bob":
			if a.Total != 1 || a.OK != 0 {
				t.Fatalf("unexpected bob summary: %+v", a)
			}
		}
	}
}

func TestOpenEmptyPathDisabled(t *testing.T) {
	s, err := Open(config.DBConfig{}, nil)
	if err != nil {
		t.Fatalf("open with empty path: %v", err)
	}
	if s != nil {
		s.Close()
		t.Fatal("expected nil store for empty path")
	}
}

func TestRecordAfterCloseNoPanic(t *testing.T) {
	s, _ := newTestStore(t)
	s.Close()
	// should not panic or block
	s.Record(Call{Account: "x", OK: true})
}

func TestCountTimeWindow(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()

	s.Record(Call{Account: "a", OK: true})
	// wait for async flush so the record's ts is stable
	time.Sleep(200 * time.Millisecond)

	now := time.Now().Unix()
	n, err := s.Count("a", now-60, now+60)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 record in window, got %d", n)
	}
}
