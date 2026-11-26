package postgres_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"bulkdoc/db"
	"bulkdoc/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Every statement in db/queries.sql must prepare against the schema.
func TestAllQueriesPrepare(t *testing.T) {
	repo, _ := newRepo(t)
	ctx := context.Background()
	queries, err := db.LoadQueries()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := repo.SQLDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range queries.Names() {
		stmt, err := sqlDB.PrepareContext(ctx, queries[name])
		if err != nil {
			t.Errorf("%s does not prepare: %v", name, err)
			continue
		}
		_ = stmt.Close()
	}
}

type briefRow struct {
	batchNo                              string
	pending, processing, success, failed int
}

// Executes the brief's query items against seeded data and checks the results.
func TestBriefQueries(t *testing.T) {
	repo, gdb := newRepo(t)
	ctx := context.Background()
	q, err := db.LoadQueries()
	if err != nil {
		t.Fatal(err)
	}

	inputs := func(prefix string, n int) []domain.DocumentInput {
		in := make([]domain.DocumentInput, n)
		for i := range in {
			in[i] = domain.DocumentInput{ExternalRef: fmt.Sprintf("%s-%d", prefix, i+1)}
		}
		return in
	}
	mk := func(no string, n int) (domain.Batch, []domain.Document) {
		b, docs, err := repo.CreateBatch(ctx, no, inputs(no, n))
		if err != nil {
			t.Fatal(err)
		}
		return b, docs
	}
	bA, docsA := mk("A", 3)
	bB, docsB := mk("B", 2)
	_, docsC := mk("C", 1)
	mk("EMPTY", 0)

	succeed := func(id, cbID string) {
		if _, err := repo.ClaimDocument(ctx, id); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.SaveCallbackAndApply(ctx, domain.Callback{CallbackID: cbID, DocumentID: id, Result: domain.StatusSuccess}); err != nil {
			t.Fatal(err)
		}
	}
	succeed(docsA[0].ID, "a1")
	d, err := repo.ClaimDocument(ctx, docsA[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkRequestFailed(ctx, d.ID, d.AttemptCount, "boom", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimDocument(ctx, docsB[0].ID); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= domain.DefaultMaxAttempts; attempt++ {
		if _, err := repo.ClaimDocument(ctx, docsB[1].ID); err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkRequestFailed(ctx, docsB[1].ID, attempt, "boom", 0); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.RetryFailed(ctx, bB.ID); err != nil {
			t.Fatal(err)
		}
	}
	succeed(docsC[0].ID, "c1")

	t.Run("q1 documents by batch_no", func(t *testing.T) {
		type row struct {
			BatchNo  string `gorm:"column:batch_no"`
			ID       int64  `gorm:"column:id"`
			Ref      string `gorm:"column:external_ref"`
			Status   string `gorm:"column:status"`
			Attempts int    `gorm:"column:attempt_count"`
		}
		var got []row
		if err := gdb.WithContext(ctx).Raw(q.Get("q1_documents_by_batch_no"), "A").Scan(&got).Error; err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0].Ref != "A-1" || got[1].Ref != "A-2" || got[2].Ref != "A-3" {
			t.Fatalf("rows = %+v", got)
		}
		if got[0].Status != "SUCCESS" || got[1].Status != "FAILED" || got[2].Status != "PENDING" {
			t.Errorf("statuses = %s %s %s", got[0].Status, got[1].Status, got[2].Status)
		}
	})

	counts := func(names any) map[string]briefRow {
		type row struct {
			ID         int64  `gorm:"column:batch_id"`
			BatchNo    string `gorm:"column:batch_no"`
			Pending    int    `gorm:"column:pending"`
			Processing int    `gorm:"column:processing"`
			Success    int    `gorm:"column:success"`
			Failed     int    `gorm:"column:failed"`
		}
		var rows []row
		if err := gdb.WithContext(ctx).Raw(q.Get("q2_status_counts_per_batch"), names).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		out := map[string]briefRow{}
		for _, r := range rows {
			out[r.BatchNo] = briefRow{r.BatchNo, r.Pending, r.Processing, r.Success, r.Failed}
		}
		return out
	}

	t.Run("q2 counts include zeros and empty batches", func(t *testing.T) {
		all := counts([]string(nil))
		want := map[string]briefRow{
			"A":     {"A", 1, 0, 1, 1},
			"B":     {"B", 0, 1, 0, 1},
			"C":     {"C", 0, 0, 1, 0},
			"EMPTY": {"EMPTY", 0, 0, 0, 0},
		}
		for no, w := range want {
			if all[no] != w {
				t.Errorf("batch %s: got %+v, want %+v", no, all[no], w)
			}
		}
		if len(all) != 4 {
			t.Errorf("got %d batches, want 4", len(all))
		}
		if only := counts([]string{"C"}); len(only) != 1 || only["C"].success != 1 {
			t.Errorf("filtered counts = %+v", only)
		}
	})

	t.Run("q3 unfinished batches", func(t *testing.T) {
		type row struct {
			ID      int64  `gorm:"column:id"`
			BatchNo string `gorm:"column:batch_no"`
		}
		var rows []row
		if err := gdb.WithContext(ctx).Raw(q.Get("q3_unfinished_batches"), 100, 0).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, r.BatchNo)
		}
		if strings.Join(got, ",") != "A,B" {
			t.Errorf("unfinished = %v, want [A B]", got)
		}
	})

	t.Run("q4 retryable failed", func(t *testing.T) {
		type row struct {
			ID int64 `gorm:"column:id"`
		}
		var rows []row
		if err := gdb.WithContext(ctx).Raw(q.Get("q4_retryable_failed"), domain.DefaultMaxAttempts, 100).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, r := range rows {
			ids = append(ids, r.ID)
		}
		if len(ids) != 1 || fmt.Sprint(ids[0]) != docsA[1].ID {
			t.Errorf("retryable = %v, want only %s (B-2 is out of attempts)", ids, docsA[1].ID)
		}
	})

	t.Run("q8 by batch and status", func(t *testing.T) {
		type row struct {
			ID int64 `gorm:"column:id"`
		}
		var rows []row
		if err := gdb.WithContext(ctx).Raw(q.Get("q8_documents_by_batch_and_status"), mustInt(t, bA.ID), "PENDING", 10).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Errorf("PENDING documents of A = %d, want 1", len(rows))
		}
	})
}

func mustInt(t testing.TB, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// With sequential scans disabled the planner must serve hot queries from the
// proposed indexes (plain-EXPLAIN workflow in docs/answers.md section D).
func TestIndexesServeHotQueries(t *testing.T) {
	repo, _ := newRepo(t)
	ctx := context.Background()
	sqlDB, err := repo.SQLDB()
	if err != nil {
		t.Fatal(err)
	}
	q, err := db.LoadQueries()
	if err != nil {
		t.Fatal(err)
	}

	var searchPath string
	if err := sqlDB.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = strings.TrimSpace(strings.Split(searchPath, ",")[0])
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	explain := func(name string) string {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		sql := "BEGIN; SET LOCAL enable_seqscan = off; EXPLAIN (GENERIC_PLAN) " + q.Get(name) + "; ROLLBACK;"
		results, err := conn.Conn().PgConn().Exec(ctx, sql).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var lines []string
		for _, res := range results {
			for _, row := range res.Rows {
				lines = append(lines, string(row[0]))
			}
		}
		return strings.Join(lines, "\n")
	}

	for name, wantIndex := range map[string]string{
		"q5_claim_pending":                 "ix_documents_batch_open",
		"q8_documents_by_batch_and_status": "ix_documents_batch_status",
		"q6_insert_callback":               "ux_callbacks_callback_id",
	} {
		if plan := explain(name); !strings.Contains(plan, wantIndex) {
			t.Errorf("%s: plan does not use %s:\n%s", name, wantIndex, plan)
		}
	}
}
