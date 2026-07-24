// Package migrations embeds the SQL schema migrations.
//
// Embedding rather than reading from disk means the migration binary and the
// integration tests carry the exact schema they were built against, with no
// dependency on the working directory or on a mounted volume.
package migrations

import "embed"

// FS holds every migration file, named <version>_<description>.<up|down>.sql.
//
//go:embed *.sql
var FS embed.FS
