// Command migrate applies the embedded PostgreSQL schema migrations.
//
// Usage:
//
//	migrate up          apply every pending migration (default)
//	migrate down [n]    roll back the n most recent migrations (default 1)
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/migrate"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"migrate","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.LoadMigrate()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	direction := "up"
	if len(args) > 0 {
		direction = args[0]
	}

	steps := 1
	if direction == "down" && len(args) > 1 {
		steps, err = strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("parse step count %q: %w", args[1], err)
		}
	}

	log := obs.NewLogger(os.Stdout, "migrate", "local", "info")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DSN:      cfg.PostgresDSN,
		MaxConns: 2,
	}, log)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	switch direction {
	case "up":
		return migrate.Up(ctx, pool, loaded, log)
	case "down":
		return migrate.Down(ctx, pool, loaded, steps, log)
	default:
		return fmt.Errorf("unknown direction %q (want up or down)", direction)
	}
}
