package store

import (
	"database/sql"
	"log/slog"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"proxy-pool/internal/config"
)

// Call records one proxy allocation for usage tracking. Recording is fully
// asynchronous: handlers push into a channel and a background worker batches
// inserts, so the request path never touches the database.
type Call struct {
	Account string
	Group   string
	ProxyID string
	Addr    string
	Kind    string
	OK      bool
}

// Store is a SQLite-backed usage recorder with async batched writes.
type Store struct {
	db      *sql.DB
	logger  *slog.Logger
	ch      chan Call
	batch   int
	flush   time.Duration
	wg      sync.WaitGroup
	closeCh chan struct{}
	closed  atomicBool
}

type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (a *atomicBool) get() bool  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }
func (a *atomicBool) set(b bool) { a.mu.Lock(); defer a.mu.Unlock(); a.v = b }

// Open initializes the SQLite database and starts the background writer.
func Open(cfg config.DBConfig, logger *slog.Logger) (*Store, error) {
	if cfg.Path == "" {
		return nil, nil // usage recording disabled
	}
	db, err := sql.Open("sqlite", cfg.Path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS calls (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ts INTEGER NOT NULL,
			account TEXT NOT NULL,
			grp TEXT NOT NULL DEFAULT '',
			proxy_id TEXT NOT NULL DEFAULT '',
			addr TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL DEFAULT '',
			ok INTEGER NOT NULL DEFAULT 1
		);
		CREATE INDEX IF NOT EXISTS idx_calls_account ON calls(account, ts);
		CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts);
	`); err != nil {
		db.Close()
		return nil, err
	}

	flush := time.Duration(cfg.FlushIntervalMs) * time.Millisecond
	if flush <= 0 {
		flush = time.Second
	}
	s := &Store{
		db:      db,
		logger:  logger,
		ch:      make(chan Call, 4096),
		batch:   cfg.BatchSize,
		flush:   flush,
		closeCh: make(chan struct{}),
	}
	if s.batch <= 0 {
		s.batch = 200
	}
	s.wg.Add(1)
	go s.run()
	return s, nil
}

// Record enqueues a call for async persistence. It never blocks the caller
// beyond a bounded buffer.
func (s *Store) Record(c Call) {
	if s == nil || s.closed.get() {
		return
	}
	select {
	case s.ch <- c:
	default:
		// buffer full: drop rather than block the request path
		if s.logger != nil {
			s.logger.Warn("usage store buffer full, dropping call", "account", c.Account)
		}
	}
}

func (s *Store) run() {
	defer s.wg.Done()
	buf := make([]Call, 0, s.batch)
	ticker := time.NewTicker(s.flush)
	defer ticker.Stop()
	flush := func() {
		if len(buf) == 0 {
			return
		}
		s.flushBatch(buf)
		buf = buf[:0]
	}
	for {
		select {
		case <-s.closeCh:
			// drain remaining buffered calls
			for {
				select {
				case c := <-s.ch:
					buf = append(buf, c)
					if len(buf) >= s.batch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case c := <-s.ch:
			buf = append(buf, c)
			if len(buf) >= s.batch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *Store) flushBatch(calls []Call) {
	tx, err := s.db.Begin()
	if err != nil {
		s.logger.Warn("usage store begin tx failed", "err", err)
		return
	}
	stmt, err := tx.Prepare(`INSERT INTO calls (ts, account, grp, proxy_id, addr, kind, ok) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		s.logger.Warn("usage store prepare failed", "err", err)
		return
	}
	now := time.Now().UnixMilli()
	ok := 1
	for _, c := range calls {
		if !c.OK {
			ok = 0
		} else {
			ok = 1
		}
		if _, err := stmt.Exec(now, c.Account, c.Group, c.ProxyID, c.Addr, c.Kind, ok); err != nil {
			s.logger.Warn("usage store insert failed", "err", err)
			break
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		s.logger.Warn("usage store commit failed", "err", err)
	}
}

// Close stops the background writer and closes the database.
func (s *Store) Close() {
	if s == nil {
		return
	}
	s.closed.set(true)
	close(s.closeCh)
	s.wg.Wait()
	s.db.Close()
}

// Count returns the number of recorded calls, optionally filtered by account
// and time window (from/to in Unix seconds, 0 = unbounded).
func (s *Store) Count(account string, from, to int64) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	q := "SELECT COUNT(*) FROM calls WHERE 1=1"
	args := []any{}
	if account != "" {
		q += " AND account = ?"
		args = append(args, account)
	}
	if from > 0 {
		q += " AND ts >= ?"
		args = append(args, from*1000)
	}
	if to > 0 {
		q += " AND ts <= ?"
		args = append(args, to*1000)
	}
	var n int64
	if err := s.db.QueryRow(q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Summary returns per-account call counts.
func (s *Store) Summary(from, to int64) ([]AccountSummary, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	q := "SELECT account, COUNT(*) AS total, SUM(CASE WHEN ok=1 THEN 1 ELSE 0 END) AS ok FROM calls WHERE 1=1"
	args := []any{}
	if from > 0 {
		q += " AND ts >= ?"
		args = append(args, from*1000)
	}
	if to > 0 {
		q += " AND ts <= ?"
		args = append(args, to*1000)
	}
	q += " GROUP BY account ORDER BY total DESC"
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AccountSummary{}
	for rows.Next() {
		var a AccountSummary
		var okv sql.NullInt64
		if err := rows.Scan(&a.Account, &a.Total, &okv); err != nil {
			return nil, err
		}
		a.OK = okv.Int64
		out = append(out, a)
	}
	return out, rows.Err()
}

// AccountSummary aggregates usage per account.
type AccountSummary struct {
	Account string `json:"account"`
	Total   int64  `json:"total"`
	OK      int64  `json:"ok"`
}
