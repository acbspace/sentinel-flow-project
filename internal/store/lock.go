package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CorrelationLockKey elects the single active correlation evaluator. The value
// is arbitrary but must be stable and must not collide with the migration lock.
const CorrelationLockKey int64 = 7_213_884_501_233_782

// CycleLock is a session-scoped PostgreSQL advisory lock, used to run a periodic
// loop on exactly one replica.
//
// The lock is held on a dedicated connection for as long as the holder keeps it,
// rather than taken and released around each cycle, because leadership has to be
// sticky: the correlation evaluator counts events since its own previous cycle,
// so a lock that changed hands every tick would make each new holder re-count
// its whole window.
//
// A lost connection releases the lock server-side, which is the desired
// behaviour — a replica that cannot talk to PostgreSQL must not keep claiming to
// be the evaluator. Acquire notices on its next call and lets another replica in.
type CycleLock struct {
	pool *pgxpool.Pool
	key  int64
	log  *slog.Logger

	// conn is non-nil exactly while this process holds the lock. The lock lives
	// on the session, so the connection must be held, not returned to the pool.
	conn *pgxpool.Conn
}

// NewCycleLock builds a lock over pool for the given advisory key.
func NewCycleLock(pool *pgxpool.Pool, key int64, log *slog.Logger) *CycleLock {
	return &CycleLock{pool: pool, key: key, log: log}
}

// Acquire reports whether this process holds the lock, taking it if it is free.
//
// It is safe to call every tick: when the lock is already held it costs one
// round trip to confirm the session is still alive. A false return is not an
// error — it means another replica is the evaluator.
func (l *CycleLock) Acquire(ctx context.Context) (bool, error) {
	if l.conn != nil {
		// The lock is only as good as the session underneath it. If the
		// connection has dropped, PostgreSQL has already released the lock and
		// another replica may hold it, so stop claiming to.
		if _, err := l.conn.Exec(ctx, "SELECT 1"); err == nil {
			return true, nil
		}
		l.log.Warn("correlation lock connection lost; will try to reacquire")
		l.conn.Release()
		l.conn = nil
	}

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire connection for cycle lock: %w", err)
	}

	var held bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", l.key).Scan(&held); err != nil {
		conn.Release()
		return false, fmt.Errorf("try cycle lock: %w", err)
	}
	if !held {
		conn.Release()
		return false, nil
	}

	l.conn = conn
	return true, nil
}

// Release drops the lock and returns the connection to the pool. It is a no-op
// when the lock is not held, so it is safe to defer unconditionally.
func (l *CycleLock) Release(ctx context.Context) {
	if l.conn == nil {
		return
	}

	// Unlock on a context that outlives a cancelled parent: on shutdown ctx is
	// already cancelled, and skipping the unlock would leave the lock held until
	// PostgreSQL noticed the session had gone.
	unlockCtx := context.WithoutCancel(ctx)
	if _, err := l.conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", l.key); err != nil {
		l.log.Error("release cycle lock", slog.String("error", err.Error()))
	}

	l.conn.Release()
	l.conn = nil
}
