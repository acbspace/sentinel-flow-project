package store

import (
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	want := Cursor{
		Time: time.Date(2026, 7, 25, 9, 30, 15, 123456789, time.UTC),
		ID:   "0921316d-4496-4568-8638-2b0ef226f850",
	}

	got, err := DecodeCursor(want.Encode())
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}

	// Nanosecond precision has to survive: two events a microsecond apart must
	// not collapse onto the same cursor position.
	if !got.Time.Equal(want.Time) {
		t.Errorf("time = %s, want %s", got.Time, want.Time)
	}
	if got.ID != want.ID {
		t.Errorf("id = %q, want %q", got.ID, want.ID)
	}
}

func TestCursorEncodeIsURLSafe(t *testing.T) {
	token := Cursor{Time: time.Now().UTC(), ID: "abc"}.Encode()

	for _, r := range token {
		safe := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_'
		if !safe {
			t.Fatalf("token %q contains %q, which needs escaping in a query string", token, r)
		}
	}
}

func TestZeroCursorEncodesToNothing(t *testing.T) {
	// An empty token is the end-of-list signal, so the zero cursor must produce
	// one rather than a token that decodes back to "start from the epoch".
	if got := (Cursor{}).Encode(); got != "" {
		t.Errorf("zero cursor encoded to %q, want empty", got)
	}
	if !(Cursor{}).IsZero() {
		t.Error("zero cursor should report IsZero")
	}
}

func TestDecodeEmptyCursorStartsAtTheBeginning(t *testing.T) {
	got, err := DecodeCursor("")
	if err != nil {
		t.Fatalf("DecodeCursor(\"\"): %v", err)
	}
	if !got.IsZero() {
		t.Errorf("empty token decoded to %+v, want the zero cursor", got)
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	for _, token := range []string{
		"not base64!!",
		"YWJj",            // "abc" — no separator
		"MjAyNi0wNy0yNSw", // truncated
		"LGFiYw",          // ",abc" — no timestamp
		"MjAyNi0wNy0yNQ",  // a date with no id
	} {
		if _, err := DecodeCursor(token); err == nil {
			t.Errorf("DecodeCursor(%q) = nil error, want a rejection", token)
		}
	}
}
