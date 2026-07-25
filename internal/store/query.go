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

// after adds the keyset condition for a cursor over (timeCol, idCol), both
// ordered descending. The row-value comparison is what lets a single index seek
// serve it; comparing the two columns separately would not.
func (b *filterBuilder) after(timeCol, idCol string, c Cursor) {
	if c.IsZero() {
		return
	}
	b.args = append(b.args, c.Time)
	timePos := len(b.args)
	b.args = append(b.args, c.ID)
	idPos := len(b.args)
	b.clauses = append(b.clauses,
		fmt.Sprintf("(%s, %s) < ($%d, $%d)", timeCol, idCol, timePos, idPos))
}

// paginate binds the trailing LIMIT and, when asked for, OFFSET. It returns the
// clause and the page size the caller requested.
//
// One more row than that is fetched. The extra row is how the caller learns
// another page exists without a second count query, and it is dropped before the
// results are returned.
func (b *filterBuilder) paginate(limit, offset int) (string, int) {
	limit = normalizeLimit(limit)

	b.args = append(b.args, limit+1)
	clause := fmt.Sprintf(" LIMIT $%d", len(b.args))

	if offset > 0 {
		b.args = append(b.args, offset)
		clause += fmt.Sprintf(" OFFSET $%d", len(b.args))
	}
	return clause, limit
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
