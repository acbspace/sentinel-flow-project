// Package store owns PostgreSQL access for SentinelFlow. Every statement is
// parameterised; no SQL is assembled from user-controlled strings.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig tunes the connection pool.
type PoolConfig struct {
	DSN               string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// NewPool opens a connection pool and verifies it can reach the database.
//
// Connecting eagerly turns a bad DSN or an unreachable database into a startup
// failure with a clear message, rather than a confusing error on the first
// event the engine tries to store.
func NewPool(ctx context.Context, cfg PoolConfig, log *slog.Logger) (*pgxpool.Pool, error) {
	if cfg.DSN == "" {
		return nil, errors.New("postgres DSN is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	}
	if cfg.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	log.Info("postgres pool ready",
		slog.String("host", poolCfg.ConnConfig.Host),
		slog.String("database", poolCfg.ConnConfig.Database),
		slog.Int("max_conns", int(poolCfg.MaxConns)),
	)

	return pool, nil
}

// IsRetryable reports whether err is worth retrying with the same input.
//
// The distinction matters for correctness, not just latency: a retryable error
// means the record has not been persisted and the Kafka offset must stay where
// it is, whereas a non-retryable error (a constraint violation, a type error)
// will fail identically forever and must not block the partition.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// A cancelled parent context is the caller shutting down, not a fault.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A per-attempt deadline usually means the database is slow or saturated.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// pgx guarantees these never reached the server, so replaying is always safe.
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}

	// The connection died, possibly mid-statement, so pgx cannot know whether
	// the server applied the write. Retrying is safe here only because the
	// insert is idempotent: a replay lands on ON CONFLICT DO NOTHING.
	if errors.Is(err, pgconn.ErrConnClosed) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization_failure
			"40P01", // deadlock_detected
			"53300", // too_many_connections
			"53400", // configuration_limit_exceeded
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03", // cannot_connect_now
			"58000", // system_error
			"58030": // io_error
			return true
		}
		// Class 08 covers every connection exception.
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" {
			return true
		}
		return false
	}

	// An unrecognised error from the driver is most often a broken connection.
	var connectErr *pgconn.ConnectError
	return errors.As(err, &connectErr)
}
