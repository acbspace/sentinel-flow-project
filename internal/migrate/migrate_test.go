package migrate_test

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/acbspace/sentinel-flow-project/internal/migrate"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

// TestLoadEmbeddedMigrations checks the real migrations directory, not a
// fixture. Everything here is a property that is cheap to assert and expensive
// to discover during a deployment: a migration with no rollback, a duplicated
// version, an empty file committed by accident.
func TestLoadEmbeddedMigrations(t *testing.T) {
	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("Load(migrations.FS) = %v", err)
	}
	if len(loaded) == 0 {
		t.Fatal("no migrations loaded")
	}

	seen := make(map[string]bool)
	previous := ""

	for _, m := range loaded {
		if m.Version == "" {
			t.Errorf("migration %q has an empty version", m.Name)
		}
		if seen[m.Version] {
			t.Errorf("version %s appears more than once", m.Version)
		}
		seen[m.Version] = true

		// Load promises version order, and Up/Down rely on it to apply and roll
		// back in the right sequence.
		if m.Version <= previous {
			t.Errorf("version %s follows %s; migrations must load in ascending order", m.Version, previous)
		}
		previous = m.Version

		// A migration nobody can roll back is a trap, which is why Load rejects a
		// missing down file. An empty one is the same trap with a file next to it.
		if strings.TrimSpace(m.UpSQL) == "" {
			t.Errorf("migration %s_%s has an empty up file", m.Version, m.Name)
		}
		if strings.TrimSpace(m.DownSQL) == "" {
			t.Errorf("migration %s_%s has an empty down file", m.Version, m.Name)
		}
	}
}

// TestLoadRejectsAMigrationWithoutARollback pins the behaviour the comment on
// Load promises, since it is the one that keeps the property above enforceable.
func TestLoadRejectsAMigrationWithoutARollback(t *testing.T) {
	fsys := fstest.MapFS{
		"0001_only_up.up.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
	}

	if _, err := migrate.Load(fsys); err == nil {
		t.Error("Load() = nil error for a migration with no down file, want an error")
	}
}
