// Package migrate applies the embedded SQL schema migrations.
//
// It is intentionally small: a version table, an advisory lock, and one
// transaction per file. A dedicated migration tool would add a dependency and a
// second CLI for behaviour this project can state in a hundred lines.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// advisoryLockKey serialises migrations across concurrently starting replicas.
// The value is arbitrary but must be stable; it is derived from the project name.
const advisoryLockKey int64 = 7_213_884_501_233_781

const createVersionTableSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT        PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Migration is one versioned schema change.
type Migration struct {
	Version string
	Name    string
	UpSQL   string
	DownSQL string
}

// Load reads every migration from fsys and returns them ordered by version.
//
// Files are named <version>_<name>.up.sql and <version>_<name>.down.sql. A
// missing down file is an error, because a migration nobody can roll back is a
// trap worth catching at build time rather than during an incident.
func Load(fsys fs.FS) ([]Migration, error) {
	upFiles, err := fs.Glob(fsys, "*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	if len(upFiles) == 0 {
		return nil, errors.New("no migrations found")
	}

	sort.Strings(upFiles)

	migrations := make([]Migration, 0, len(upFiles))
	for _, upFile := range upFiles {
		base := strings.TrimSuffix(upFile, ".up.sql")

		version, name, found := strings.Cut(base, "_")
		if !found || version == "" {
			return nil, fmt.Errorf("migration %q must be named <version>_<name>.up.sql", upFile)
		}

		upSQL, err := fs.ReadFile(fsys, upFile)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", upFile, err)
		}

		downFile := base + ".down.sql"
		downSQL, err := fs.ReadFile(fsys, downFile)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", downFile, err)
		}

		migrations = append(migrations, Migration{
			Version: version,
			Name:    name,
			UpSQL:   string(upSQL),
			DownSQL: string(downSQL),
		})
	}

	return migrations, nil
}

// Up applies every migration that has not been applied yet.
//
// It is safe to run concurrently and repeatedly: an advisory lock serialises
// competing runners, and each migration's version row is written in the same
// transaction as its DDL, so a crash mid-migration leaves neither applied.
func Up(ctx context.Context, pool *pgxpool.Pool, migrations []Migration, log *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Release on a context that outlives a cancelled parent, otherwise a
		// cancelled run would leave the lock held until the session ends.
		unlockCtx := context.WithoutCancel(ctx)
		if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey); err != nil {
			log.Error("release migration lock", slog.String("error", err.Error()))
		}
	}()

	if _, err := conn.Exec(ctx, createVersionTableSQL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	pending := 0
	for _, m := range migrations {
		if _, ok := applied[m.Version]; ok {
			log.Debug("migration already applied", slog.String("version", m.Version), slog.String("name", m.Name))
			continue
		}

		log.Info("applying migration", slog.String("version", m.Version), slog.String("name", m.Name))

		if err := runInTx(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.UpSQL); err != nil {
				return fmt.Errorf("execute migration %s: %w", m.Version, err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", m.Version); err != nil {
				return fmt.Errorf("record migration %s: %w", m.Version, err)
			}
			return nil
		}); err != nil {
			return err
		}

		pending++
	}

	log.Info("migrations up to date",
		slog.Int("applied_now", pending),
		slog.Int("total", len(migrations)),
	)
	return nil
}

// Down rolls back the most recently applied migrations, newest first.
func Down(ctx context.Context, pool *pgxpool.Pool, migrations []Migration, steps int, log *slog.Logger) error {
	if steps < 1 {
		return errors.New("steps must be at least 1")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		unlockCtx := context.WithoutCancel(ctx)
		if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey); err != nil {
			log.Error("release migration lock", slog.String("error", err.Error()))
		}
	}()

	if _, err := conn.Exec(ctx, createVersionTableSQL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	rolledBack := 0
	for i := len(migrations) - 1; i >= 0 && rolledBack < steps; i-- {
		m := migrations[i]
		if _, ok := applied[m.Version]; !ok {
			continue
		}

		log.Info("rolling back migration", slog.String("version", m.Version), slog.String("name", m.Name))

		if err := runInTx(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.DownSQL); err != nil {
				return fmt.Errorf("execute rollback %s: %w", m.Version, err)
			}
			if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE version = $1", m.Version); err != nil {
				return fmt.Errorf("clear migration record %s: %w", m.Version, err)
			}
			return nil
		}); err != nil {
			return err
		}

		rolledBack++
	}

	log.Info("rollback complete", slog.Int("rolled_back", rolledBack))
	return nil
}

func appliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]struct{})
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}

	return applied, nil
}

// runInTx runs fn inside a transaction, rolling back on any error.
func runInTx(ctx context.Context, conn *pgxpool.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := fn(tx); err != nil {
		// Roll back on a context that outlives cancellation so the connection is
		// not left with an open transaction.
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
