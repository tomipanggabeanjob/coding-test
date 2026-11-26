// Package postgres implements repository.Repository on PostgreSQL using GORM.
//
// Concurrency-critical statements still come from the graded db/queries.sql
// (embedded) and run via GORM Raw/Exec so SKIP LOCKED, idempotent callback
// inserts and guarded status updates stay exactly as documented. GORM supplies
// the ORM layer: models, transactions, and typed scans.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"bulkdoc/db"
	"bulkdoc/internal/domain"
	"bulkdoc/internal/repository"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var _ repository.Repository = (*Repo)(nil)

const (
	pgUniqueViolation     = "23505"
	pgInvalidTextRep      = "22P02"
	pgInvalidJSON         = "22032"
	constraintBatchNo     = "batches_batch_no_key"
	constraintExternalRef = "documents_batch_id_external_ref_key"
)

func requiredQueries() []string {
	return []string{
		"q5_claim_pending", "q6_insert_callback", "q7_apply_callback",
		"app_insert_batch", "app_insert_documents", "app_get_batch", "app_get_document",
		"app_list_documents", "app_claim_document", "app_mark_accepted", "app_mark_failed",
		"app_release_claim", "app_lock_document", "app_retry_failed", "app_recover_stuck",
		"app_batch_status", "app_batch_exists", "app_document_exists",
	}
}

// Repo is the GORM-backed repository.
type Repo struct {
	db *gorm.DB
	q  db.Queries
}

// Open builds a GORM DB for the given Postgres DSN (pgx under the hood).
func Open(dsn string) (*gorm.DB, error) {
	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true, // we open explicit transactions where needed
		PrepareStmt:            false,
	})
	if err != nil {
		return nil, fmt.Errorf("open gorm postgres: %w", err)
	}
	return gdb, nil
}

// New wraps an open GORM DB. The caller owns the underlying *sql.DB lifetime.
func New(gdb *gorm.DB) (*Repo, error) {
	if gdb == nil {
		return nil, errors.New("gorm DB is required")
	}
	q, err := db.LoadQueries()
	if err != nil {
		return nil, err
	}
	if err := q.Require(requiredQueries()...); err != nil {
		return nil, err
	}
	return &Repo{db: gdb, q: q}, nil
}

// DB exposes the GORM handle (tests / Prepare checks).
func (r *Repo) DB() *gorm.DB { return r.db }

// SQLDB returns the pooled database/sql handle.
func (r *Repo) SQLDB() (*sql.DB, error) { return r.db.DB() }

func parseID(s string) (int64, bool) {
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

func formatID(id int64) string { return strconv.FormatInt(id, 10) }

func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == pgInvalidTextRep || pgErr.Code == pgInvalidJSON) {
		return fmt.Errorf("postgres %s: %w: %s", op, domain.ErrInvalidInput, pgErr.Message)
	}
	return fmt.Errorf("postgres %s: %w", op, err)
}

func isUnique(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == constraint
}

func (r *Repo) session(ctx context.Context) *gorm.DB {
	return r.db.WithContext(ctx)
}

func (r *Repo) exists(ctx context.Context, query string, id int64) (bool, error) {
	var ok bool
	if err := r.session(ctx).Raw(r.q.Get(query), id).Scan(&ok).Error; err != nil {
		return false, err
	}
	return ok, nil
}

func sortDocs(docs []domain.Document) {
	slices.SortFunc(docs, func(a, b domain.Document) int {
		ai, _ := parseID(a.ID)
		bi, _ := parseID(b.ID)
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		}
		return 0
	})
}

func rowsToDocs(rows []documentRow) []domain.Document {
	docs := make([]domain.Document, len(rows))
	for i := range rows {
		docs[i] = rows[i].toDomain()
	}
	sortDocs(docs)
	return docs
}

func (r *Repo) CreateBatch(ctx context.Context, batchNo string, inputs []domain.DocumentInput) (domain.Batch, []domain.Document, error) {
	refs := make([]string, len(inputs))
	payloads := make([]string, len(inputs))
	for i, in := range inputs {
		if in.ExternalRef == "" {
			return domain.Batch{}, nil, fmt.Errorf("%w: empty external_ref", domain.ErrInvalidInput)
		}
		refs[i], payloads[i] = in.ExternalRef, string(in.Payload)
	}

	var (
		batch domain.Batch
		docs  []domain.Document
	)
	err := r.session(ctx).Transaction(func(tx *gorm.DB) error {
		var row batchRow
		if err := tx.Raw(r.q.Get("app_insert_batch"), batchNo).Scan(&row).Error; err != nil {
			if isUnique(err, constraintBatchNo) {
				return fmt.Errorf("batch %q: %w", batchNo, domain.ErrBatchExists)
			}
			return wrap("insert batch", err)
		}
		batch = row.toDomain()
		batch.BatchNo = batchNo // RETURNING only id, created_at

		var docRows []documentRow
		if err := tx.Raw(r.q.Get("app_insert_documents"), row.ID, refs, payloads).Scan(&docRows).Error; err != nil {
			if isUnique(err, constraintExternalRef) {
				return fmt.Errorf("%w: duplicate external_ref in batch", domain.ErrInvalidInput)
			}
			return wrap("insert documents", err)
		}
		docs = rowsToDocs(docRows)
		return nil
	})
	if err != nil {
		return domain.Batch{}, nil, err
	}
	return batch, docs, nil
}

func (r *Repo) GetBatch(ctx context.Context, batchID string) (domain.Batch, error) {
	id, ok := parseID(batchID)
	if !ok {
		return domain.Batch{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	var row batchRow
	err := r.session(ctx).Raw(r.q.Get("app_get_batch"), id).Scan(&row).Error
	if err != nil {
		return domain.Batch{}, wrap("get batch", err)
	}
	if row.ID == 0 {
		return domain.Batch{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	return row.toDomain(), nil
}

func (r *Repo) ClaimPending(ctx context.Context, batchID string, limit int) ([]domain.Document, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: claim limit must be positive", domain.ErrInvalidInput)
	}
	id, ok := parseID(batchID)
	if !ok {
		return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	var rows []documentRow
	if err := r.session(ctx).Raw(r.q.Get("q5_claim_pending"), id, limit).Scan(&rows).Error; err != nil {
		return nil, wrap("claim pending", err)
	}
	docs := rowsToDocs(rows)
	if len(docs) == 0 {
		found, err := r.exists(ctx, "app_batch_exists", id)
		if err != nil {
			return nil, wrap("claim pending", err)
		}
		if !found {
			return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
		}
	}
	return docs, nil
}

func (r *Repo) ClaimDocument(ctx context.Context, docID string) (domain.Document, error) {
	id, ok := parseID(docID)
	if !ok {
		return domain.Document{}, fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	var row documentRow
	if err := r.session(ctx).Raw(r.q.Get("app_claim_document"), id).Scan(&row).Error; err != nil {
		return domain.Document{}, wrap("claim document", err)
	}
	if row.ID != 0 {
		return row.toDomain(), nil
	}
	cur, err := r.GetDocument(ctx, docID)
	if err != nil {
		return domain.Document{}, err
	}
	return domain.Document{}, fmt.Errorf("document %q is %s with %d/%d attempts: %w",
		docID, cur.Status, cur.AttemptCount, cur.MaxAttempts, domain.ErrNotProcessable)
}

func (r *Repo) guardedUpdate(ctx context.Context, op, docID, query string, args ...any) error {
	id, ok := parseID(docID)
	if !ok {
		return fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	res := r.session(ctx).Exec(r.q.Get(query), append([]any{id}, args...)...)
	if res.Error != nil {
		return wrap(op, res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	found, err := r.exists(ctx, "app_document_exists", id)
	if err != nil {
		return wrap(op, err)
	}
	if !found {
		return fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	return nil
}

func (r *Repo) MarkRequestAccepted(ctx context.Context, docID string, attempt int) error {
	return r.guardedUpdate(ctx, "mark request accepted", docID, "app_mark_accepted", attempt)
}

func (r *Repo) MarkRequestFailed(ctx context.Context, docID string, attempt int, reason string, retryAfter time.Duration) error {
	return r.guardedUpdate(ctx, "mark request failed", docID, "app_mark_failed", attempt, reason, retryAfter.Milliseconds())
}

func (r *Repo) ReleaseClaim(ctx context.Context, docID string, attempt int) error {
	return r.guardedUpdate(ctx, "release claim", docID, "app_release_claim", attempt)
}

func (r *Repo) SaveCallbackAndApply(ctx context.Context, cb domain.Callback) (domain.CallbackOutcome, error) {
	if cb.Result != domain.StatusSuccess && cb.Result != domain.StatusFailed {
		return "", fmt.Errorf("%w: callback result must be SUCCESS or FAILED, got %q", domain.ErrInvalidInput, cb.Result)
	}
	docID, ok := parseID(cb.DocumentID)
	if !ok {
		return "", fmt.Errorf("document %q: %w", cb.DocumentID, domain.ErrDocumentNotFound)
	}

	var outcome domain.CallbackOutcome
	err := r.session(ctx).Transaction(func(tx *gorm.DB) error {
		var status string
		res := tx.Raw(r.q.Get("app_lock_document"), docID).Scan(&status)
		if res.Error != nil {
			return wrap("lock document", res.Error)
		}
		if res.RowsAffected == 0 || status == "" {
			return fmt.Errorf("document %q: %w", cb.DocumentID, domain.ErrDocumentNotFound)
		}

		var raw []byte
		if len(cb.RawBody) > 0 {
			raw = cb.RawBody
		}
		var callbackRowID int64
		ins := tx.Raw(r.q.Get("q6_insert_callback"), cb.CallbackID, docID, string(cb.Result), raw).Scan(&callbackRowID)
		if ins.Error != nil {
			return wrap("insert callback", ins.Error)
		}
		if ins.RowsAffected == 0 || callbackRowID == 0 {
			outcome = domain.CallbackDuplicate
			return nil
		}

		if domain.DocStatus(status) == domain.StatusPending {
			return fmt.Errorf("callback %q for document %q in status %s: %w",
				cb.CallbackID, cb.DocumentID, status, domain.ErrInvalidTransition)
		}

		lastErr := ""
		if cb.Result == domain.StatusFailed {
			lastErr = "principal reported FAILED"
		}
		var updated int64
		upd := tx.Raw(r.q.Get("q7_apply_callback"), docID, string(cb.Result), lastErr).Scan(&updated)
		if upd.Error != nil {
			return wrap("apply callback", upd.Error)
		}
		if upd.RowsAffected == 0 || updated == 0 {
			outcome = domain.CallbackIgnored
		} else {
			outcome = domain.CallbackApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

func (r *Repo) RetryFailed(ctx context.Context, batchID string) (int, error) {
	id, ok := parseID(batchID)
	if !ok {
		return 0, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	res := r.session(ctx).Exec(r.q.Get("app_retry_failed"), id)
	if res.Error != nil {
		return 0, wrap("retry failed", res.Error)
	}
	if res.RowsAffected == 0 {
		found, err := r.exists(ctx, "app_batch_exists", id)
		if err != nil {
			return 0, wrap("retry failed", err)
		}
		if !found {
			return 0, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
		}
	}
	return int(res.RowsAffected), nil
}

func (r *Repo) RecoverStuck(ctx context.Context, lease time.Duration) (int, error) {
	res := r.session(ctx).Exec(r.q.Get("app_recover_stuck"), lease.Milliseconds())
	if res.Error != nil {
		return 0, wrap("recover stuck", res.Error)
	}
	return int(res.RowsAffected), nil
}

func (r *Repo) BatchStatus(ctx context.Context, batchID string) (domain.BatchStatusView, error) {
	id, ok := parseID(batchID)
	if !ok {
		return domain.BatchStatusView{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	var (
		bid                                                  int64
		view                                                 domain.BatchStatusView
		pending, processing, success, failed, retryableCount int
	)
	res := r.session(ctx).Raw(r.q.Get("app_batch_status"), id).Row()
	err := res.Scan(&bid, &view.BatchNo, &view.Total, &pending, &processing, &success, &failed, &retryableCount, &view.Started)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.BatchStatusView{}, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	if err != nil {
		return domain.BatchStatusView{}, wrap("batch status", err)
	}
	view.BatchID = formatID(bid)
	view.RetryableFailed = retryableCount
	view.Counts = map[domain.DocStatus]int{
		domain.StatusPending: pending, domain.StatusProcessing: processing,
		domain.StatusSuccess: success, domain.StatusFailed: failed,
	}
	view.Status = domain.DeriveBatchStatus(view)
	return view, nil
}

func (r *Repo) GetDocument(ctx context.Context, docID string) (domain.Document, error) {
	id, ok := parseID(docID)
	if !ok {
		return domain.Document{}, fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	var row documentRow
	if err := r.session(ctx).Raw(r.q.Get("app_get_document"), id).Scan(&row).Error; err != nil {
		return domain.Document{}, wrap("get document", err)
	}
	if row.ID == 0 {
		return domain.Document{}, fmt.Errorf("document %q: %w", docID, domain.ErrDocumentNotFound)
	}
	return row.toDomain(), nil
}

func (r *Repo) ListDocuments(ctx context.Context, batchID string) ([]domain.Document, error) {
	id, ok := parseID(batchID)
	if !ok {
		return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
	}
	var rows []documentRow
	if err := r.session(ctx).Raw(r.q.Get("app_list_documents"), id).Scan(&rows).Error; err != nil {
		return nil, wrap("list documents", err)
	}
	docs := rowsToDocs(rows)
	if len(docs) == 0 {
		found, err := r.exists(ctx, "app_batch_exists", id)
		if err != nil {
			return nil, wrap("list documents", err)
		}
		if !found {
			return nil, fmt.Errorf("batch %q: %w", batchID, domain.ErrBatchNotFound)
		}
	}
	return docs, nil
}
