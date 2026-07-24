package store_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// TestIsRetryable pins the classification that decides whether a failed insert
// blocks its Kafka offset (retryable) or is reported as permanent. Getting this
// wrong either loses events or stalls a partition forever, so each class is
// covered explicitly.
func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "no error",
			err:  nil,
			want: false,
		},
		{
			name: "cancelled context is a shutdown, not a fault",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "deadline exceeded suggests a slow or saturated database",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "network failure",
			err:  &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "wrapped network failure",
			err:  fmt.Errorf("insert event: %w", &net.OpError{Op: "read", Err: errors.New("reset by peer")}),
			want: true,
		},
		{
			name: "connection closed underneath the query",
			err:  pgconn.ErrConnClosed,
			want: true,
		},
		{
			name: "serialization failure",
			err:  &pgconn.PgError{Code: "40001"},
			want: true,
		},
		{
			name: "deadlock",
			err:  &pgconn.PgError{Code: "40P01"},
			want: true,
		},
		{
			name: "too many connections",
			err:  &pgconn.PgError{Code: "53300"},
			want: true,
		},
		{
			name: "server shutting down",
			err:  &pgconn.PgError{Code: "57P01"},
			want: true,
		},
		{
			name: "cannot connect now, as during recovery",
			err:  &pgconn.PgError{Code: "57P03"},
			want: true,
		},
		{
			name: "class 08 connection exception",
			err:  &pgconn.PgError{Code: "08006"},
			want: true,
		},
		{
			name: "wrapped retryable postgres error",
			err:  fmt.Errorf("insert event abc: %w", &pgconn.PgError{Code: "40001"}),
			want: true,
		},
		{
			name: "check constraint violation will fail identically forever",
			err:  &pgconn.PgError{Code: "23514"},
			want: false,
		},
		{
			name: "unique violation is a permanent decision",
			err:  &pgconn.PgError{Code: "23505"},
			want: false,
		},
		{
			name: "invalid text representation is a data bug",
			err:  &pgconn.PgError{Code: "22P02"},
			want: false,
		},
		{
			name: "undefined table means the migration never ran",
			err:  &pgconn.PgError{Code: "42P01"},
			want: false,
		},
		{
			name: "syntax error is a code bug",
			err:  &pgconn.PgError{Code: "42601"},
			want: false,
		},
		{
			name: "an unrecognised plain error is not retried",
			err:  errors.New("something unexpected"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := store.IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNewPoolRejectsBadConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dsn  string
	}{
		{name: "empty DSN", dsn: ""},
		{name: "unparseable DSN", dsn: "://not-a-dsn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := store.NewPool(context.Background(), store.PoolConfig{DSN: tc.dsn}, discardLogger())
			if err == nil {
				pool.Close()
				t.Fatalf("NewPool(%q) = nil error, want a startup failure", tc.dsn)
			}
		})
	}
}
