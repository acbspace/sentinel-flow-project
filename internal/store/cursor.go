package store

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// Cursor marks a position in a list ordered by a timestamp with an id
// tiebreaker. It is the pagination key for events and incidents alike.
//
// OFFSET was the previous answer and does not survive this table growing:
// PostgreSQL still walks and discards every skipped row, so page 5,000 costs
// five thousand pages of work. A cursor pins the position by value, so every
// page costs the same and pages do not shift when new rows arrive at the front.
type Cursor struct {
	Time time.Time
	ID   string
}

// Encode renders a cursor as an opaque, URL-safe token.
//
// It is encoded rather than exposed as two query parameters so that callers
// treat it as a position rather than as an ordering they can construct: the
// columns behind it are free to change.
func (c Cursor) Encode() string {
	if c.ID == "" && c.Time.IsZero() {
		return ""
	}
	raw := c.Time.UTC().Format(time.RFC3339Nano) + "," + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses a token produced by Encode. An empty token yields the zero
// cursor, meaning "start at the beginning".
func DecodeCursor(token string) (Cursor, error) {
	if token == "" {
		return Cursor{}, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor is not a valid token")
	}

	rawTime, id, found := strings.Cut(string(decoded), ",")
	if !found || id == "" {
		return Cursor{}, fmt.Errorf("cursor is malformed")
	}

	ts, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor carries an invalid timestamp")
	}

	return Cursor{Time: ts, ID: id}, nil
}

// IsZero reports whether the cursor points at the start of the list.
func (c Cursor) IsZero() bool { return c.Time.IsZero() && c.ID == "" }
