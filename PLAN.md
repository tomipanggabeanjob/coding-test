# Implementation Plan: Bulk Document Processing Service (Go)

Source: `Full_stack_developer_golang_test.pdf` (Indonesian). This plan is in English. Deliverables may be written in either language; the docs can be switched to Indonesian if preferred.

---

## 1. Goals and Scope

Build a Go service that accepts batches of documents, sends each document to the external Principal API with at most 3 requests in flight per instance, receives asynchronous SUCCESS/FAILED callbacks, and reports batch progress. The service must be correct under concurrency and pass `go build ./...` and `go test -race -count=10 ./...`.

**Must have**
- Six public functions with the contract names kept: `CreateBatch`, `ProcessBatch`, `ProcessDocument`, `HandleCallback`, `RetryFailed`, `GetBatchStatus`.
- Global concurrency limit of 3 across all batches.
- Max 3 attempts per document (1 initial + 2 retries).
- SUCCESS is terminal and is never reprocessed.
- Idempotent callbacks, including concurrent duplicates.
- Late FAILED callbacks cannot overwrite SUCCESS.
- `context.Context` support (cancel and timeout) through the whole flow.
- Sentinel errors (`ErrBatchNotFound`, `ErrDocumentNotFound`, and so on) wrapped with `%w`.
- No mutable global variables. No lock held during `PrincipalClient.Submit`.
- Unit tests covering all 8 points in section VI of the brief.
- PostgreSQL schema and queries (`db/schema.sql`, `db/queries.sql`).
- Written answers (`docs/answers.md`) covering sections VIII.A to VIII.D.
- `README.md` covering how to run, assumptions, design decisions, and what to improve next.

**Nice to have (optional, do only after the must-haves are green)**
- PostgreSQL repository (pgx or `database/sql`) with a testcontainers integration test.
- HTTP callback endpoint with validation and correct status codes.
- Exponential backoff with jitter.
- Graceful shutdown and lease-based recovery of stuck PROCESSING documents.
- Metrics and structured logging (`log/slog`).
- Benchmark for `ProcessBatch`.

---

## 2. Assumptions (to confirm or document in README)

1. One process is one "instance". The 3-request limit is enforced in memory per process. Across several instances, the limit is per instance (the brief says "dalam satu instance").
2. `attempt_count` is incremented when a document is claimed (PENDING to PROCESSING), not when Submit returns. This makes the rule "retry only if `attempt_count < 3`" work out to exactly 3 Submit calls.
3. Every callback carries a unique `CallbackID` from Principal. This is the idempotency key for callbacks. The same ID arriving twice is a duplicate.
4. A callback can only change a document that is PROCESSING. Callbacks for PENDING, FAILED, or SUCCESS documents are either a duplicate (no-op) or a stale/unknown event (recorded but ignored, or rejected with a sentinel error). Decision needed: see section 5.6.
5. `RetryFailed` is a manual or scheduled call, not automatic. Automatic retry is an optional extra.
6. Documents are identified by an external `DocumentID` the client supplies, unique within a batch.
7. `max_attempts` is 3 as a constant, also stored per row so it can change later without code changes (see schema decision D3).

---

## 3. Architecture

### 3.1 Layers

```
cmd/server            (optional) wiring, HTTP callback endpoint, graceful shutdown
internal/domain       entities, statuses, state transitions, sentinel errors
internal/service      the six use cases, concurrency control, retry policy
internal/principal    PrincipalClient interface + HTTP implementation + mock
internal/repository   Repository interface
internal/repository/memory     mutex-based implementation (used by tests)
internal/repository/postgres   pgx/database-sql implementation (optional)
db/schema.sql
db/queries.sql
docs/answers.md
README.md
```

Dependency direction: `service` depends on interfaces (`Repository`, `PrincipalClient`, clock/ID generator if needed). `domain` depends on nothing. Repositories and the Principal client are injected through the constructor. No package-level mutable state.

### 3.2 Core types (sketch)

```go
type DocStatus string // PENDING, PROCESSING, SUCCESS, FAILED
type BatchStatus string // PENDING, IN_PROGRESS, COMPLETED

type Batch struct {
    ID        string
    BatchNo   string
    CreatedAt time.Time
}

type Document struct {
    ID             string
    BatchID        string
    Status         DocStatus
    AttemptCount   int
    MaxAttempts    int
    LockedAt       *time.Time
    NextRetryAt    *time.Time
    LastError      string
    Payload        []byte
}

type Callback struct {
    CallbackID string // idempotency key from Principal
    DocumentID string
    Result     DocStatus // SUCCESS or FAILED
    ReceivedAt time.Time
}
```

Repository interface (keep it small and transactional in intent):

```go
type Repository interface {
    CreateBatch(ctx context.Context, b Batch, docs []Document) error
    ClaimPending(ctx context.Context, batchID string, limit int) ([]Document, error)
    MarkRequestAccepted(ctx context.Context, docID string) error        // PROCESSING, waiting callback
    MarkRequestFailed(ctx context.Context, docID string, reason string) error
    ReleaseClaim(ctx context.Context, docID string) error               // cancel: back to PENDING, attempt-1
    SaveCallbackAndApply(ctx context.Context, cb Callback) (applied bool, err error)
    RetryFailed(ctx context.Context, batchID string) (int, error)
    BatchStatus(ctx context.Context, batchID string) (BatchStatusView, error)
    GetDocument(ctx context.Context, docID string) (Document, error)
}
```

`SaveCallbackAndApply` is the single place where the callback idempotency and the conditional status update happen together, in one atomic unit (transaction in Postgres, one mutex section in memory).

### 3.3 State machine (enforced in `domain`)

| From | To | Trigger | Guard |
|---|---|---|---|
| PENDING | PROCESSING | worker claims | status = PENDING, attempt_count incremented |
| PROCESSING | PROCESSING (waiting) | Submit accepted | none (status already PROCESSING) |
| PROCESSING | SUCCESS | callback SUCCESS | status = PROCESSING |
| PROCESSING | FAILED | callback FAILED | status = PROCESSING |
| PROCESSING | FAILED | Submit error | status = PROCESSING |
| PROCESSING | PENDING | context canceled before Submit | status = PROCESSING, attempt_count decremented |
| FAILED | PENDING | RetryFailed | attempt_count < max_attempts |
| SUCCESS | none | terminal | any transition is rejected |

Encode this as a `domain.CanTransition(from, to)` function plus a table test. Repositories re-check the guard inside the atomic update (defense in depth).

---

## 4. Concurrency Design

### 4.1 Global limit of 3 in-flight Submit calls

Use a single semaphore owned by the `Service` value, not by `ProcessBatch`:

```go
type Service struct {
    repo      Repository
    principal PrincipalClient
    sem       chan struct{} // capacity 3, created in NewService
}
```

Acquire before `Submit`, release after it returns. Acquire must select on `ctx.Done()` so cancellation does not block. The semaphore is never held while waiting on the database.

### 4.2 ProcessBatch

1. Loop: claim up to N PENDING documents (atomic claim, section 4.3).
2. For each claimed document, start a worker (bounded by the global semaphore) that calls `ProcessDocument`.
3. Use `errgroup` (`golang.org/x/sync/errgroup`) or `sync.WaitGroup` to wait for all started workers. Wait for everything started before returning, so no goroutine leaks.
4. Stop when no PENDING documents remain or `ctx` is done.
5. On `ctx` cancellation: return `ctx.Err()` (wrapped, so `errors.Is(err, context.Canceled)` works). Documents claimed but not yet submitted are released back to PENDING (section 4.4).

Because claim and submit are both gated, two concurrent `ProcessBatch` calls cannot exceed 3 in-flight requests in total.

### 4.3 Atomic claim

- Memory repo: `sync.Mutex` around "find first PENDING, set PROCESSING, increment attempt, set locked_at". The mutex is released before any Submit.
- Postgres repo: `UPDATE ... WHERE id IN (SELECT id ... FOR UPDATE SKIP LOCKED LIMIT $n) RETURNING *` (query 5 in `db/queries.sql`).

### 4.4 Cancellation and no stuck PROCESSING

- A document is claimed (PROCESSING) but the context is canceled before `Submit` is called: call `ReleaseClaim`, which moves it back to PENDING and decrements `attempt_count`. The release uses a fresh context derived from `context.Background()` with a short timeout, so a canceled context does not prevent the cleanup. This is the only place that does this on purpose.
- A document whose `Submit` returned an error: `MarkRequestFailed` (FAILED, `attempt_count` stays incremented).
- A document whose `Submit` succeeded: it stays PROCESSING and waits for the callback. If the callback never arrives, the lease/recovery path handles it (section 8, optional).

### 4.5 Callback idempotency under concurrency

- `SaveCallbackAndApply` runs in one atomic section:
  1. Insert the callback with `callback_id` as unique key. If it already exists, return `applied=false, nil` (duplicate, no effect).
  2. Conditionally update the document: only if `status = PROCESSING` (SUCCESS and late callbacks cannot overwrite).
  3. Commit.
- Two identical callbacks at the same time: in memory, the mutex serializes them; in Postgres, the unique index makes the second `INSERT ... ON CONFLICT DO NOTHING` return zero rows, so the second one is a no-op.
- The late-FAILED rule is therefore enforced by the conditional `WHERE status <> 'SUCCESS'` (or `= 'PROCESSING'`) update, not by application timing.

### 4.6 Lock discipline

- Never hold a lock across `Submit` or any network call.
- Memory repo: hold the mutex only for in-memory state changes.
- No goroutine waits on a channel while holding a mutex.

---

## 5. Use Cases (behavior)

### 5.1 CreateBatch(ctx, batchNo, docs)
- Validate: non-empty batch number, at least 1 document, unique document IDs within the batch.
- Insert batch and all documents with status PENDING, `attempt_count = 0`, `max_attempts = 3`, in one transaction.
- Errors: validation error (sentinel), duplicate batch number (sentinel `ErrBatchExists`).

### 5.2 ProcessBatch(ctx, batchID)
- Returns `ErrBatchNotFound` for unknown batch.
- Processes all PENDING documents of the batch (section 4.2).
- Does not touch SUCCESS documents (they are never claimed, because claim only selects PENDING).

### 5.3 ProcessDocument(ctx, docID)
- Claimed-by-id variant: only proceeds if the document is PENDING. If it is already PROCESSING, SUCCESS or FAILED, return a sentinel such as `ErrNotProcessable` (or a no-op result). This is what makes test 3 pass: concurrent calls for the same ID result in only one `Submit`.
- Acquire semaphore, call `PrincipalClient.Submit(ctx, req)` with the request ID (section 7.2).
- Accepted: `MarkRequestAccepted` (stays PROCESSING).
- Error: `MarkRequestFailed`.
- Release semaphore.

### 5.4 HandleCallback(ctx, cb)
- Validate: non-empty `CallbackID`, `DocumentID`, and `Result` in {SUCCESS, FAILED}.
- Delegate to `SaveCallbackAndApply`.
- Return `ErrDocumentNotFound` if the document does not exist.
- A duplicate callback returns success (idempotent) with no side effects.

### 5.5 RetryFailed(ctx, batchID)
- For each FAILED document with `attempt_count < max_attempts` (and `next_retry_at` is null or `<= now`), set PENDING.
- Returns the number of documents moved.
- Documents with `attempt_count >= max_attempts` stay FAILED (final).

### 5.6 Open decision: callback for a document not in PROCESSING
Options:
- (a) Ignore and return nil, record the callback for audit.
- (b) Return a sentinel `ErrInvalidTransition`.

Recommendation: (a) for SUCCESS documents (duplicate or late, safe no-op) and (b) for PENDING documents (should not happen, surface it). Document the choice in README.

### 5.7 GetBatchStatus(ctx, batchID)
- Returns counts per status and derived batch status:
  - `COMPLETED`: no PENDING, PROCESSING, or retryable FAILED documents.
  - `PENDING`: no document started yet.
  - `IN_PROGRESS`: otherwise.
- Uses a consistent read (single query or read under the same lock in memory).

---

## 6. Retry Policy

- Rule: a document may be retried only while `attempt_count < max_attempts` (3).
- Trace: claim (attempt 1) fails, then RetryFailed (1 < 3) claims again (attempt 2) fails, then RetryFailed (2 < 3) claims again (attempt 3) fails, then RetryFailed (3 < 3 is false) leaves it FAILED. Total Submit calls: 3.
- Optional (later): exponential backoff with jitter. Store `next_retry_at`; `RetryFailed` only moves documents where `next_retry_at <= now()`. Inject a clock for determinism.

---

## 7. Principal Client Contract

### 7.1 Interface

```go
type PrincipalClient interface {
    Submit(ctx context.Context, req SubmitRequest) error // nil = accepted (result comes by callback)
}
```

`Submit` returns nil when Principal accepts the request, or an error otherwise. Errors are wrapped with `%w`. Treat timeouts and 5xx as failures; 4xx may be treated as permanent (optional refinement).

### 7.2 Request ID
- `RequestID = fmt.Sprintf("%s:%d", docID, attemptCount)`. Sent to Principal so it can deduplicate repeated submissions. Document this in `docs/answers.md` (section VIII.C).

### 7.3 Test doubles
- `mockPrincipal` with:
  - `atomic.Int32` in-flight counter and recorded max.
  - Optional barrier channel to hold calls until N are in flight.
  - Scripted results (always fail, fail N times, block until context done).
  - Call counter per document ID (mutex-protected map, test-only).

---

## 8. Optional Extras (after must-haves)

- **HTTP callback endpoint**: `POST /callbacks`. Validate JSON, return 200 for accepted or duplicate, 400 for invalid body, 404 for unknown document, 500 for internal errors. Keep the handler thin.
- **Lease recovery**: a background sweeper resets PROCESSING documents with `locked_at < now() - lease` to FAILED (so RetryFailed can pick them up), or to PENDING if attempts remain. Lease must be longer than the expected callback latency.
- **Graceful shutdown**: on SIGTERM, stop claiming, cancel context, wait for workers with a deadline.
- **Metrics**: counters for submits, failures, duplicate callbacks, and gauge for in-flight. Use `log/slog` with batch and document IDs.

---

## 9. Test Plan (maps to section VI of the brief)

All tests must be deterministic: no `time.Sleep` for synchronization. Use channels, barriers, `sync.WaitGroup`, and `context` deadlines. Run with `go test -race -count=10 ./...`.

| # | Requirement | Test name (suggested) | How it is made deterministic |
|---|---|---|---|
| 1 | Max 3 in flight | `TestProcessBatch_GlobalConcurrencyLimit` | ≥20 docs; mock Submit increments an atomic counter, records max, then waits on a barrier channel that releases when 3 are in flight. Assert `maxInFlight == 3` (and `<= 3` as the hard check). Release barrier after reaching 3. |
| 1b | Limit is global across batches | `TestConcurrentBatches_SharedLimit` | Two `ProcessBatch` calls on different batches run at once; same barrier technique; assert max is still 3. |
| 2 | Retry limit | `TestRetry_MaxThreeAttempts` | Mock always fails. Loop: `ProcessBatch` then `RetryFailed` until nothing moves. Assert `Submit` called exactly 3 times per document. Then one more `RetryFailed` must not cause a 4th call. |
| 3 | No double processing | `TestProcessDocument_SingleClaimUnderRace` | N goroutines call `ProcessDocument(sameID)` after a start barrier. Assert `Submit` called exactly once. |
| 4 | Idempotent callback | `TestHandleCallback_Duplicate` | Same `CallbackID` sent sequentially 5x and in parallel 20x. Assert final status correct, one callback row, one side effect (e.g. counter in memory repo). |
| 5 | Late FAILED does not overwrite SUCCESS | `TestHandleCallback_LateFailedKeepsSuccess` | Document reaches SUCCESS via callback A, then callback B (different ID, FAILED) arrives. Assert status still SUCCESS. |
| 6 | No data races | `TestMixedOperations_Race` | Run ProcessBatch, HandleCallback, RetryFailed, and GetBatchStatus concurrently in a loop with a fixed number of iterations; rely on `-race`. Use a `sync.WaitGroup`, not sleeps. |
| 7 | Context cancellation | `TestProcessBatch_ContextCancel` | Mock Submit blocks on `ctx.Done()` or a barrier. Cancel after N in flight. Assert `errors.Is(err, context.Canceled)`. Use `goleak` (or count `runtime.NumGoroutine` before and after, with retries bounded). Assert no document left in PROCESSING (should be PENDING). |
| 8 | SUCCESS not reprocessed | `TestProcessBatch_SuccessNotReprocessed` | Call ProcessBatch twice. Mock records Submit per doc ID. Assert second call issues no Submit for SUCCESS docs. |
| B | Submit timeout | `TestSubmit_Timeout` | Mock Submit blocks until ctx done. Call with `context.WithTimeout(10ms)` or a deadline. Assert document becomes FAILED and semaphore is released (a follow-up call can proceed). |
| B2 | Benchmark | `BenchmarkProcessBatch` | Mock principal returns immediately; measure throughput. |

Flakiness notes for README: no sleeps, all concurrency synchronized through barriers and channels, mock is fully controlled, timeouts are generous and used only as failure guards (not as expected timing), and race detector runs on every test.

---

## 10. PostgreSQL Schema (`db/schema.sql`)

```sql
CREATE TABLE batches (
    id          BIGSERIAL PRIMARY KEY,
    batch_no    TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE documents (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT      NOT NULL REFERENCES batches(id),
    external_ref  TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING','PROCESSING','SUCCESS','FAILED')),
    attempt_count INT         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts  INT         NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    locked_at     TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    last_error    TEXT,
    payload       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, external_ref),
    CHECK (attempt_count <= max_attempts),
    CHECK (status <> 'PROCESSING' OR locked_at IS NOT NULL)
);

CREATE TABLE callbacks (
    id           BIGSERIAL PRIMARY KEY,
    callback_id  TEXT        NOT NULL,
    document_id  BIGINT      NOT NULL REFERENCES documents(id),
    result       TEXT        NOT NULL CHECK (result IN ('SUCCESS','FAILED')),
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_body     JSONB
);

CREATE UNIQUE INDEX ux_callbacks_callback_id ON callbacks (callback_id);
```

Design decisions to explain in README and answers:
- **D1.** `UNIQUE (batch_id, external_ref)` prevents duplicate documents in a batch.
- **D2.** `callbacks.callback_id` is the idempotency key. A unique index is the only guarantee that holds under concurrency; a check-then-insert in application code is not enough.
- **D3.** `max_attempts` is stored per row, so policy changes do not need a code deploy and existing rows keep their limit.
- **D4.** `CHECK` constraints move invariants into the database, so a buggy service cannot store a PROCESSING row without a lock timestamp.
- **D5.** `locked_at` is the lease marker for stuck-document recovery (optional extra).
- **D6.** `batch_no` is unique, so `CreateBatch` can be retried safely by the client.
- **D7.** `callbacks` keeps an audit trail; `raw_body` helps debugging.

---

## 11. PostgreSQL Queries (`db/queries.sql`)

Every query has a short comment and uses positional parameters (`$1`, `$2`, ...). No hard-coded literals for IDs or limits.

1. **All documents by batch_no, stable order**
   ```sql
   SELECT b.batch_no, d.id, d.external_ref, d.status, d.attempt_count, d.created_at
   FROM documents d
   JOIN batches b ON b.id = d.batch_id
   WHERE b.batch_no = $1
   ORDER BY d.id;
   ```

2. **Count per status per batch, including zeros**
   Use `FILTER` on a fixed status list so every status appears even when count is 0. Use `LEFT JOIN` so batches with no documents still appear.

3. **Batches not finished** (has PENDING, PROCESSING, or retryable FAILED)
   Use `EXISTS` on documents filtered by status and `attempt_count < max_attempts`.

4. **Retryable FAILED documents** (`attempt_count < $1`, `next_retry_at IS NULL OR next_retry_at <= now()`), ordered by id, limited by `$2`.

5. **Atomic claim of PENDING documents**
   ```sql
   WITH claimed AS (
     SELECT id FROM documents
     WHERE batch_id = $1 AND status = 'PENDING'
     ORDER BY id
     LIMIT $2
     FOR UPDATE SKIP LOCKED
   )
   UPDATE documents d
   SET status = 'PROCESSING',
       attempt_count = d.attempt_count + 1,
       locked_at = now(),
       updated_at = now()
   FROM claimed c
   WHERE d.id = c.id
   RETURNING d.*;
   ```
   Note: the planner may not guarantee row order on `RETURNING`; sort in Go if needed.

6. **Idempotent callback insert**
   ```sql
   INSERT INTO callbacks (callback_id, document_id, result, raw_body)
   VALUES ($1, $2, $3, $4)
   ON CONFLICT (callback_id) DO NOTHING
   RETURNING id;
   ```
   Zero rows returned means duplicate. One row returned means new. The app distinguishes the two by whether `RETURNING` produced a row, and only applies the document update when it did.

7. **Conditional document update, in one transaction with the callback insert**
   ```sql
   BEGIN;
   -- step 1: insert callback (query 6); if no row returned, COMMIT and stop
   -- step 2:
   UPDATE documents
   SET status = $3, last_error = $4, locked_at = NULL, updated_at = now()
   WHERE id = $2
     AND status = 'PROCESSING'   -- never overwrites SUCCESS; late callbacks are no-ops
   RETURNING id;
   COMMIT;
   ```
   Using `status = 'PROCESSING'` is stricter than `status <> 'SUCCESS'` and also blocks illegal PENDING or FAILED changes. Explain this choice in answers.

8. **Index proposal for lookup by batch and status**
   Index design is in section 12. Query shape: `WHERE batch_id = $1 AND status = $2`.

---

## 12. Index Proposal

- **I1** `documents (batch_id, status)`: serves queries 2, 3, and per-batch status checks. Leading column is `batch_id` because almost all access is scoped to one batch; `status` second for equality filters and counts.
- **I2 (partial)** `documents (id) WHERE status = 'PENDING'`: serves the claim query (5). Small, hot, and kept tiny because most rows eventually become SUCCESS and leave the index.
- **I3 (partial)** `documents (batch_id, id) WHERE status IN ('PENDING','FAILED')`: serves RetryFailed and "not finished" queries. Pair with the status condition in the query so the planner can use it.
- **I4** `callbacks (callback_id)` unique: already created in schema; required for idempotency.
- **I5** `callbacks (document_id)`: for audit lookup by document (optional).

Trade-offs to write in answers.md:
- Each extra index adds write cost on every status change (PENDING to PROCESSING to SUCCESS), because status is updated often.
- Partial indexes only help if the query predicate matches the index predicate exactly.
- Before adding indexes, verify with `EXPLAIN (ANALYZE, BUFFERS)` (section VIII.D).

---

## 13. Written Answers (`docs/answers.md`)

Structure, with concrete examples (not textbook definitions):

- **A. SELECT then UPDATE race.** Walk through two workers reading the same PENDING row, both submitting to Principal (double processing, Principal sees two requests). Mitigations: (1) atomic `UPDATE ... WHERE status = 'PENDING' RETURNING` or claim with `FOR UPDATE SKIP LOCKED`; (2) optimistic version column with `WHERE version = $n`. Include the request ID so Principal can deduplicate.
- **B. Transaction vs row lock vs unique constraint.** What each solves, when each is not enough (transaction alone does not stop two transactions from inserting the same callback; row lock does not cover rows that do not exist yet; unique constraint does not protect multi-row invariants). How they combine in the design: unique index for callbacks, row lock in claim, transaction around callback insert plus document update.
- **C. Recovery when Submit succeeded but DB update failed.** Cover: document stuck in PROCESSING; request ID sent to Principal so retry is deduplicated; reconciliation by polling Principal status or waiting for callback; lease via `locked_at` to take over stuck rows; outbox pattern (write "submit intent" in the same transaction as the claim, a separate sender publishes). Be explicit: realistic guarantee is at-least-once delivery to Principal with idempotency on Principal's side, not exactly-once.
- **D. EXPLAIN ANALYZE.** How to read Seq Scan vs Index Scan, estimated vs actual rows (big gap means stale statistics, run `ANALYZE`), `loops` multiplying cost, `Buffers: shared hit/read`, signs of trouble (Seq Scan on large table with selective filter, `Sort Method: external merge Disk`, missing index), fixes (index, `ANALYZE`, rewrite query). Risk: running `EXPLAIN ANALYZE` on `UPDATE`/`DELETE` actually executes it. Use `BEGIN; EXPLAIN ANALYZE ...; ROLLBACK;` on a replica or staging, or use plain `EXPLAIN` first.

---

## 14. Repository Layout

```
.
├── go.mod
├── cmd/server/main.go           (optional)
├── internal/
│   ├── domain/
│   │   ├── batch.go
│   │   ├── document.go
│   │   ├── status.go            state transitions + tests
│   │   └── errors.go            sentinel errors
│   ├── service/
│   │   ├── service.go           NewService, six use cases, semaphore
│   │   ├── process.go           ProcessBatch, ProcessDocument
│   │   ├── callback.go          HandleCallback
│   │   ├── retry.go             RetryFailed
│   │   ├── status.go            GetBatchStatus
│   │   └── *_test.go
│   ├── principal/
│   │   ├── client.go            interface + HTTP client
│   │   └── mock_test.go         or internal/principal/principaltest
│   └── repository/
│       ├── memory/              mutex-based repo + tests
│       └── postgres/            optional
├── db/
│   ├── schema.sql
│   └── queries.sql
├── docs/answers.md
└── README.md
```

---

## 15. Implementation Phases

Work in this order so each step is testable before the next.

1. **Scaffold**: `go mod init`, folders, `domain` types, sentinel errors, state transition table + tests.
2. **Memory repository**: mutex-based implementation of the `Repository` interface, with unit tests for claim, callback apply, retry rules.
3. **Principal mock**: inflight counter, barrier helper, scripted results.
4. **Service core**: `CreateBatch`, `GetBatchStatus`, `HandleCallback`, `RetryFailed`.
5. **Processing**: semaphore, `ProcessDocument`, `ProcessBatch`, cancellation and release logic.
6. **Test plan items 1 to 8** (section 9), each as its own test, all run with `-race -count=10`.
7. **Timeout test and benchmark.**
8. **SQL**: `db/schema.sql`, `db/queries.sql`. Validate against a local Postgres (`psql -f`) if available.
9. **Write `docs/answers.md`** (sections A to D).
10. **README.md**: how to run, assumptions, design decisions, what to improve.
11. **Optional extras**: Postgres repo with integration test, HTTP callback, backoff, lease recovery, metrics.
12. **Final checks**: `go build ./...`, `go vet ./...`, `go test -race -count=10 ./...`, `gofmt -l .` must be empty, check for leftover `time.Sleep` in tests.

---

## 16. Definition of Done

- [x] `go build ./...` passes
- [x] `go vet ./...` clean
- [x] `go test -race -count=10 ./...` passes repeatedly with no failures
- [x] All 8 test points from section VI have a named test
- [x] Timeout test and benchmark exist
- [x] No mutable package-level variables
- [x] No lock held during `Submit`
- [x] Six functions present with the contract names
- [x] Sentinel errors used with `%w` and checked with `errors.Is` in tests
- [x] `db/schema.sql` and `db/queries.sql` present, all 8 query items answered, parameterized
- [x] `docs/answers.md` covers VIII.A to VIII.D with concrete examples
- [x] `README.md` covers run instructions, assumptions, design decisions, and next improvements

---

## 17. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Flaky concurrency tests | Barriers and channels instead of sleeps; run `-count=10`; assert on invariants, not timing |
| Goroutine leaks on cancel | Wait for all workers before returning; `goleak` in tests |
| Deadlock by holding a lock during Submit | Lock only around state changes; code review checklist |
| Cancel leaves documents PROCESSING | `ReleaseClaim` with a fresh context; test 7 asserts no stuck rows |
| Scope creep from optional items | Finish must-haves and tests first; optional items last |
| Memory and Postgres behavior diverge | Same repository contract tests run against both implementations |

---

## 18. Improvements to Mention in README (if more time)

- Persistent lease recovery and automatic retry worker.
- Exponential backoff with jitter and a dead-letter state.
- Distributed limit across instances (Redis or Postgres-based token bucket) instead of per-process semaphore.
- Outbox pattern for submission to remove the window between Submit and DB write.
- Metrics dashboards and tracing across Submit and callback.
- Stronger request validation and payload size limits.
