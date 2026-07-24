package store

import (
	"fmt"
	"strings"
)

// Pagination bounds shared by every list endpoint. A caller may ask for fewer
// than the default, but never for an unbounded page: an omitted or non-positive
// limit becomes the default, and anything above the ceiling is clamped to it.
const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// rowScanner is the one method pgx.Row and pgx.Rows share, so a single scan
// helper can serve both a single-row lookup and an iterated result set.
type rowScanner interface {
	Scan(dest ...any) error
}

// filterBuilder accumulates parameterised WHERE clauses so that every value a
// caller supplies is bound as a placeholder. No SQL is ever assembled from a
// caller-controlled string, which is what keeps these dynamic queries injection
// safe.
type filterBuilder struct {
	clauses []string
	args    []any
}

// add appends one condition. condFmt must contain a single %d, which is filled
// with the 1-based position of val in the argument list, e.g. "status = $%d".
func (b *filterBuilder) add(condFmt string, val any) {
	b.args = append(b.args, val)
	b.clauses = append(b.clauses, fmt.Sprintf(condFmt, len(b.args)))
}

// where renders the accumulated conditions, or the empty string if there are
// none.
func (b *filterBuilder) where() string {
	if len(b.clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(b.clauses, " AND ")
}

// paginate binds the (clamped) limit and offset as the final two arguments and
// returns the trailing clause. It is called after every condition so the
// placeholder numbers line up.
func (b *filterBuilder) paginate(limit, offset int) string {
	limit = normalizeLimit(limit)
	if offset < 0 {
		offset = 0
	}
	b.args = append(b.args, limit)
	limitClause := fmt.Sprintf(" LIMIT $%d", len(b.args))
	b.args = append(b.args, offset)
	offsetClause := fmt.Sprintf(" OFFSET $%d", len(b.args))
	return limitClause + offsetClause
}

func normalizeLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultListLimit
	case limit > maxListLimit:
		return maxListLimit
	default:
		return limit
	}
}
