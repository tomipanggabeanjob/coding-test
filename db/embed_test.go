package db

import (
	"strings"
	"testing"
)

func TestParseQueries(t *testing.T) {
	q, err := ParseQueries(`
-- header ignored
-- name: first
-- a comment
SELECT 1;

-- name: second
SELECT $1;
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 || !strings.Contains(q["first"], "SELECT 1;") || q["second"] != "SELECT $1;" {
		t.Fatalf("unexpected result: %#v", q)
	}
}

func TestParseQueriesErrors(t *testing.T) {
	for name, text := range map[string]string{
		"duplicate": "-- name: a\nSELECT 1;\n-- name: a\nSELECT 2;",
		"empty":     "-- name: a\n-- name: b\nSELECT 1;",
		"no name":   "-- name:\nSELECT 1;",
	} {
		if _, err := ParseQueries(text); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestEmbeddedFilesLoad(t *testing.T) {
	q, err := LoadQueries()
	if err != nil {
		t.Fatal(err)
	}
	// The brief asks for eight query items.
	for _, prefix := range []string{"q1_", "q2_", "q3_", "q4_", "q5_", "q6_", "q7_", "q8_"} {
		found := false
		for _, n := range q.Names() {
			found = found || strings.HasPrefix(n, prefix)
		}
		if !found {
			t.Errorf("no query with prefix %s in queries.sql", prefix)
		}
	}
	if !strings.Contains(Schema(), "CREATE TABLE documents") {
		t.Error("schema.sql not embedded")
	}
}
