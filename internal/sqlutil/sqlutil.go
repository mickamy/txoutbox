package sqlutil

import (
	"database/sql"
	"strings"
)

func QuoteIdentifier(name, quote string) string {
	return quote + escapeIdentifier(name, quote) + quote
}

func escapeIdentifier(name, quote string) string {
	if name == "" {
		return ""
	}
	escapedQuote := quote + quote
	return strings.ReplaceAll(name, quote, escapedQuote)
}

func NullableString(ns sql.NullString) *string {
	if ns.Valid {
		val := ns.String
		return &val
	}
	return nil
}
