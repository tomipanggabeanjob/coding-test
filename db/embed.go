// Package db embeds the SQL deliverables (schema.sql, queries.sql) so the
// Postgres repository runs exactly the statements that are documented there.
package db

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

// files is read-only embedded data (the one package-level declaration //go:embed requires).
//
//go:embed schema.sql queries.sql
var files embed.FS

const nameMarker = "-- name:"

// Schema returns the contents of schema.sql.
func Schema() string {
	b, err := files.ReadFile("schema.sql")
	if err != nil {
		panic(fmt.Sprintf("db: embedded schema.sql missing: %v", err)) // build-time invariant
	}
	return string(b)
}

// Queries maps a statement name to its SQL text.
type Queries map[string]string

// Get returns the named statement or panics: a missing name is a programming
// error that LoadQueries' callers verify at startup with Require.
func (q Queries) Get(name string) string {
	sql, ok := q[name]
	if !ok {
		panic("db: unknown query " + name)
	}
	return sql
}

// Require fails if any of the given names is missing.
func (q Queries) Require(names ...string) error {
	var missing []string
	for _, n := range names {
		if _, ok := q[n]; !ok {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("db: queries.sql lacks statements: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Names returns every statement name, sorted.
func (q Queries) Names() []string {
	names := make([]string, 0, len(q))
	for n := range q {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// LoadQueries parses the embedded queries.sql.
func LoadQueries() (Queries, error) {
	b, err := files.ReadFile("queries.sql")
	if err != nil {
		return nil, fmt.Errorf("read queries.sql: %w", err)
	}
	return ParseQueries(string(b))
}

// ParseQueries splits SQL text into statements introduced by "-- name: <id>".
// Everything before the first marker is ignored; comment lines between a marker
// and the statement are kept (they are valid SQL comments).
func ParseQueries(text string) (Queries, error) {
	queries := make(Queries)
	var name string
	var body strings.Builder
	flush := func() error {
		if name == "" {
			return nil
		}
		sql := strings.TrimSpace(body.String())
		if sql == "" {
			return fmt.Errorf("db: query %q has no SQL", name)
		}
		if _, dup := queries[name]; dup {
			return fmt.Errorf("db: duplicate query name %q", name)
		}
		queries[name] = sql
		return nil
	}

	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), nameMarker); ok {
			if err := flush(); err != nil {
				return nil, err
			}
			name = strings.TrimSpace(rest)
			if name == "" {
				return nil, fmt.Errorf("db: empty query name")
			}
			body.Reset()
			continue
		}
		if name != "" {
			body.WriteString(line)
			body.WriteByte('\n')
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return queries, nil
}
