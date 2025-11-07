package sqlutil_test

import (
	"database/sql"
	"testing"

	"github.com/mickamy/txoutbox/internal/sqlutil"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	if got := sqlutil.QuoteIdentifier(`foo"bar`); got != `"foo""bar"` {
		t.Fatalf("QuoteIdentifier(%q) = %s, want %s", `foo"bar`, got, `"foo""bar"`)
	}
}

func TestEscapeIdentifier(t *testing.T) {
	t.Parallel()
	if got := sqlutil.EscapeIdentifier(`f"o`); got != `f""o` {
		t.Fatalf("EscapeIdentifier(%q) = %s, want %s", `f"o`, got, `f""o`)
	}
}

func TestNullableString(t *testing.T) {
	t.Parallel()
	val := sql.NullString{String: "hello", Valid: true}
	ptr := sqlutil.NullableString(val)
	if ptr == nil || *ptr != "hello" {
		t.Fatalf("NullableString(%v) = %v, want %q", val, ptr, "hello")
	}

	invalid := sql.NullString{}
	if got := sqlutil.NullableString(invalid); got != nil {
		t.Fatalf("NullableString(%v) = %v, want nil", invalid, got)
	}
}
