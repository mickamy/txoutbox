package sqlutil

import "database/sql"

// QuoteIdentifier returns a double-quoted identifier suitable for PostgreSQL.
func QuoteIdentifier(name string) string {
	return `"` + EscapeIdentifier(name) + `"`
}

// EscapeIdentifier doubles internal quotes for safe quoting.
func EscapeIdentifier(name string) string {
	if name == "" {
		return ""
	}
	res := make([]rune, 0, len(name))
	for _, r := range name {
		if r == '"' {
			res = append(res, '"', '"')
			continue
		}
		res = append(res, r)
	}
	return string(res)
}

// NullableString converts sql.NullString into *string.
func NullableString(ns sql.NullString) *string {
	if ns.Valid {
		val := ns.String
		return &val
	}
	return nil
}
