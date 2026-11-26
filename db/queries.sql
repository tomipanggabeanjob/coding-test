-- Queries for the bulk document processing service.
--
-- Every statement is introduced by a "-- name: <identifier>" marker. The
-- Postgres repository loads this very file (embedded into the binary), so what
-- is documented here is what runs. All statements use positional parameters.
--
-- Sections:
--   1. The eight queries requested in the brief (names q1_ .. q8_)
--   2. Application statements used by the repository implementation

-- ===========================================================================
-- 1. Brief queries
-- ===========================================================================

-- name: q1_documents_by_batch_no
-- All documents of a batch, looked up by batch_no, in a stable order.
-- $1 = batch_no
SELECT b.batch_no, d.id, d.external_ref, d.status, d.attempt_count, d.created_at
FROM documents d
JOIN batches b ON b.id = d.batch_id
WHERE b.batch_no = $1
ORDER BY d.id;

-- name: q2_status_counts_per_batch
-- Document count per status per batch. FILTER keeps every status as a column
-- (0 when absent) and the LEFT JOIN keeps batches that have no documents at all.
-- $1 = array of batch numbers, or NULL for every batch
SELECT b.id AS batch_id,
       b.batch_no,
       count(d.id) FILTER (WHERE d.status = 'PENDING')    AS pending,
       count(d.id) FILTER (WHERE d.status = 'PROCESSING') AS processing,
       count(d.id) FILTER (WHERE d.status = 'SUCCESS')    AS success,
       count(d.id) FILTER (WHERE d.status = 'FAILED')     AS failed
FROM batches b
LEFT JOIN documents d ON d.batch_id = b.id
WHERE $1::text[] IS NULL OR b.batch_no = ANY ($1::text[])
GROUP BY b.id, b.batch_no
ORDER BY b.id;

-- name: q3_unfinished_batches
-- Batches that still have work: PENDING, PROCESSING, or FAILED with attempts
-- left. EXISTS stops at the first matching document instead of counting all.
-- $1 = max rows, $2 = offset
SELECT b.id, b.batch_no, b.created_at
FROM batches b
WHERE EXISTS (
    SELECT 1
    FROM documents d
    WHERE d.batch_id = b.id
      AND (d.status IN ('PENDING', 'PROCESSING')
           OR (d.status = 'FAILED' AND d.attempt_count < d.max_attempts))
)
ORDER BY b.id
LIMIT $1 OFFSET $2;

-- name: q4_retryable_failed
-- FAILED documents that may be retried now: attempts left and no backoff pending.
-- $1 = max attempts (exclusive upper bound for attempt_count), $2 = max rows
SELECT d.id, d.batch_id, d.external_ref, d.attempt_count, d.next_retry_at
FROM documents d
WHERE d.status = 'FAILED'
  AND d.attempt_count < $1
  AND (d.next_retry_at IS NULL OR d.next_retry_at <= now())
ORDER BY d.id
LIMIT $2;

-- name: q5_claim_pending
-- Atomic claim: pick PENDING rows of one batch, skip rows other workers have
-- locked, and flip them to PROCESSING in the same statement. Two concurrent
-- claimers can never get the same row. RETURNING order is not guaranteed.
-- $1 = batch id, $2 = max rows
WITH claimed AS (
    SELECT id
    FROM documents
    WHERE batch_id = $1
      AND status = 'PENDING'
      AND attempt_count < max_attempts
    ORDER BY id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE documents d
SET status        = 'PROCESSING',
    attempt_count = d.attempt_count + 1,
    locked_at     = now(),
    next_retry_at = NULL,
    updated_at    = now()
FROM claimed c
WHERE d.id = c.id
RETURNING d.*;

-- name: q6_insert_callback
-- Idempotent callback insert (brief VII.A.6).
-- Unique index ux_callbacks_callback_id (see db/schema.sql) is the only race-free
-- idempotency gate; a check-then-insert in application code is not enough.
--
-- How the app tells "new" vs "duplicate":
--   * INSERT ... ON CONFLICT DO NOTHING RETURNING id
--   * one row returned  → this delivery is new → continue to q7 in the same tx
--   * zero rows (pgx.ErrNoRows) → duplicate CallbackID → commit nothing further,
--     report CallbackDuplicate to the caller (HTTP 200, no side effect)
-- Two identical callbacks arriving at once: the unique index serialises them;
-- exactly one INSERT wins, the other gets DO NOTHING.
-- $1 = callback_id, $2 = document id, $3 = result, $4 = raw body (jsonb, may be NULL)
INSERT INTO callbacks (callback_id, document_id, result, raw_body)
VALUES ($1, $2, $3, $4)
ON CONFLICT (callback_id) DO NOTHING
RETURNING id;

-- name: q7_apply_callback
-- Conditional document update (brief VII.A.7), always in the SAME transaction as
-- q6 and only after q6 returned a row. The brief suggests
--   WHERE id = $1 AND status <> 'SUCCESS'
-- We use the stricter form status = 'PROCESSING' so a stray callback also cannot
-- move PENDING or FAILED. Late FAILED after SUCCESS therefore updates 0 rows.
--
-- Full callback transaction (implemented in postgres.SaveCallbackAndApply):
--   BEGIN;
--   -- lock the document row (app_lock_document) so status cannot flip mid-flight
--   -- q6: insert callback; if no row returned → COMMIT and stop (duplicate)
--   -- if status was PENDING → ROLLBACK (ErrInvalidTransition)
--   -- q7: UPDATE ... WHERE status = 'PROCESSING' RETURNING id
--   --      0 rows → CallbackIgnored (e.g. late FAILED); still COMMIT the audit row
--   COMMIT;
-- $1 = document id, $2 = new status (SUCCESS | FAILED), $3 = last_error
UPDATE documents
SET status     = $2,
    last_error = NULLIF($3, ''),
    locked_at  = NULL,
    updated_at = now()
WHERE id = $1
  AND status = 'PROCESSING'
RETURNING id;

-- name: q8_documents_by_batch_and_status
-- Lookup by batch + status (brief VII.A.8). Index proposal (created in schema.sql):
--
--   I1  documents (batch_id, status)
--       Leading batch_id because almost every access is scoped to one batch;
--       status second for equality filters and the FILTER counts in q2.
--
--   I2  PARTIAL documents (id) WHERE status = 'PENDING'
--       Hot path for unscoped PENDING work; stays tiny because SUCCESS rows leave it.
--
--   I3  PARTIAL documents (batch_id, id) WHERE status IN ('PENDING', 'FAILED')
--       Serves per-batch claim (q5), RetryFailed, and "unfinished" checks (q3).
--       Partial predicate must match the query predicate for the planner to use it.
--
--   I4  UNIQUE callbacks (callback_id) — required for q6 idempotency under concurrency.
--   I5  callbacks (document_id) — optional audit lookup by document.
--   I6  PARTIAL documents (locked_at) WHERE status = 'PROCESSING' — lease recovery.
--
-- Trade-offs: each extra index costs writes on every status change
-- (PENDING → PROCESSING → SUCCESS/FAILED). Partial indexes limit that cost to the
-- small active subset. Prefer EXPLAIN (ANALYZE, BUFFERS) on realistic data before
-- adding more; see docs/answers.md section D.
-- $1 = batch id, $2 = status, $3 = max rows
SELECT d.id, d.external_ref, d.attempt_count, d.updated_at
FROM documents d
WHERE d.batch_id = $1
  AND d.status = $2
ORDER BY d.id
LIMIT $3;

-- ===========================================================================
-- 2. Application statements (repository implementation)
-- ===========================================================================

-- name: app_insert_batch
-- $1 = batch_no
INSERT INTO batches (batch_no) VALUES ($1) RETURNING id, created_at;

-- name: app_insert_documents
-- Bulk insert of one batch's documents; max_attempts comes from the column default.
-- $1 = batch id, $2 = external refs, $3 = payloads as JSON text ('' = no payload)
INSERT INTO documents (batch_id, external_ref, payload)
SELECT $1, r.external_ref, NULLIF(r.payload, '')::jsonb
FROM unnest($2::text[], $3::text[]) AS r (external_ref, payload)
RETURNING *;

-- name: app_get_batch
-- $1 = batch id
SELECT id, batch_no, created_at FROM batches WHERE id = $1;

-- name: app_get_document
-- $1 = document id
SELECT d.* FROM documents d WHERE d.id = $1;

-- name: app_list_documents
-- $1 = batch id
SELECT d.* FROM documents d WHERE d.batch_id = $1 ORDER BY d.id;

-- name: app_claim_document
-- Claim one specific PENDING document (same guard as q5).
-- $1 = document id
UPDATE documents d
SET status        = 'PROCESSING',
    attempt_count = d.attempt_count + 1,
    locked_at     = now(),
    next_retry_at = NULL,
    updated_at    = now()
WHERE d.id = $1
  AND d.status = 'PENDING'
  AND d.attempt_count < d.max_attempts
RETURNING d.*;

-- name: app_mark_accepted
-- Principal accepted the request: stay PROCESSING and restart the lease. A no-op
-- when a fast callback already settled the document or the attempt moved on.
-- $1 = document id, $2 = attempt
UPDATE documents
SET locked_at  = now(),
    last_error = NULL,
    updated_at = now()
WHERE id = $1
  AND status = 'PROCESSING'
  AND attempt_count = $2;

-- name: app_mark_failed
-- Submit failed: PROCESSING -> FAILED, guarded by status and attempt so it can
-- never overwrite a SUCCESS that arrived first. A positive backoff sets next_retry_at.
-- $1 = document id, $2 = attempt, $3 = reason, $4 = backoff in milliseconds (0 = none)
UPDATE documents
SET status        = 'FAILED',
    last_error    = $3,
    locked_at     = NULL,
    next_retry_at = CASE WHEN $4::bigint > 0
                         THEN now() + $4::bigint * interval '1 millisecond'
                         ELSE NULL END,
    updated_at    = now()
WHERE id = $1
  AND status = 'PROCESSING'
  AND attempt_count = $2;

-- name: app_release_claim
-- Cancel before/while submitting: PROCESSING -> PENDING and give the attempt back.
-- $1 = document id, $2 = attempt
UPDATE documents
SET status        = 'PENDING',
    attempt_count = attempt_count - 1,
    locked_at     = NULL,
    updated_at    = now()
WHERE id = $1
  AND status = 'PROCESSING'
  AND attempt_count = $2;

-- name: app_lock_document
-- First step of the callback transaction: serialise callbacks per document.
-- $1 = document id
SELECT status FROM documents WHERE id = $1 FOR UPDATE;

-- name: app_retry_failed
-- FAILED -> PENDING for documents with attempts left and no backoff pending.
-- $1 = batch id
UPDATE documents
SET status        = 'PENDING',
    next_retry_at = NULL,
    updated_at    = now()
WHERE batch_id = $1
  AND status = 'FAILED'
  AND attempt_count < max_attempts
  AND (next_retry_at IS NULL OR next_retry_at <= now());

-- name: app_recover_stuck
-- Lease recovery: PROCESSING documents not settled within the lease become FAILED
-- so RetryFailed can re-queue them while attempts remain.
-- $1 = lease in milliseconds
UPDATE documents
SET status     = 'FAILED',
    last_error = 'lease expired',
    locked_at  = NULL,
    updated_at = now()
WHERE status = 'PROCESSING'
  AND locked_at < now() - $1::bigint * interval '1 millisecond';

-- name: app_batch_status
-- Consistent single-statement snapshot of one batch (one MVCC snapshot).
-- $1 = batch id
SELECT b.id,
       b.batch_no,
       count(d.id)                                                     AS total,
       count(d.id) FILTER (WHERE d.status = 'PENDING')                 AS pending,
       count(d.id) FILTER (WHERE d.status = 'PROCESSING')              AS processing,
       count(d.id) FILTER (WHERE d.status = 'SUCCESS')                 AS success,
       count(d.id) FILTER (WHERE d.status = 'FAILED')                  AS failed,
       count(d.id) FILTER (WHERE d.status = 'FAILED'
                             AND d.attempt_count < d.max_attempts)     AS retryable_failed,
       coalesce(bool_or(d.attempt_count > 0), false)                   AS started
FROM batches b
LEFT JOIN documents d ON d.batch_id = b.id
WHERE b.id = $1
GROUP BY b.id, b.batch_no;

-- name: app_batch_exists
-- $1 = batch id
SELECT EXISTS (SELECT 1 FROM batches WHERE id = $1);

-- name: app_document_exists
-- $1 = document id
SELECT EXISTS (SELECT 1 FROM documents WHERE id = $1);
