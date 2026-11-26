# Written answers (brief sections VIII.A–VIII.D)

Concrete walkthroughs tied to this codebase (`bulkdoc`), not textbook definitions.

---

## A. SELECT then UPDATE race

**Scenario.** Two workers both need a PENDING document. A naïve flow:

```sql
-- worker 1 and worker 2, interleaved
SELECT id FROM documents WHERE batch_id = $1 AND status = 'PENDING' LIMIT 1;
-- both see document 42
UPDATE documents SET status = 'PROCESSING' WHERE id = 42;
-- both succeed
```

Both then call `Principal.Submit`. Principal sees two requests for the same document; callbacks may fight; progress counters lie.

**What we do instead**

1. **Atomic claim** (Postgres `q5_claim_pending` / `app_claim_document`): one statement that selects under `FOR UPDATE SKIP LOCKED` and updates to `PROCESSING` with `attempt_count + 1`. Two claimers cannot return the same row.
2. **Memory repo**: the same transition runs under one mutex; the mutex is released *before* `Submit` (see `service.send`).
3. **Request ID** `fmt.Sprintf("%s:%d", docID, attemptCount)` (see `domain.Document.RequestID`). If a rare double-submit still reaches Principal, Principal can deduplicate by that key. Releasing a claim decrements `attempt_count`, so a reclaimed document reuses the ID of the attempt that never completed.

Optimistic concurrency (`WHERE version = $n`) is a valid alternative for some updates; for claim we prefer `SKIP LOCKED` because workers should take *different* rows, not retry the same contested row.

---

## B. Transaction vs row lock vs unique constraint

| Tool | What it solves | What it does *not* solve |
|---|---|---|
| **Transaction** | Several statements commit or roll back together (callback insert + document update). | Alone, two transactions can both insert the “same” logical callback if there is no uniqueness rule. |
| **Row lock** (`FOR UPDATE` / `SKIP LOCKED`) | Serialises updates on rows that already exist (claim). | Cannot lock a row that does not exist yet (two inserts of a new callback). |
| **Unique constraint** | Makes “this callback_id already exists” a database fact under concurrency. | Does not protect multi-row invariants by itself (e.g. “only PROCESSING may become SUCCESS”). |

**How they combine here**

- **Claim**: row lock inside `FOR UPDATE SKIP LOCKED` + single `UPDATE … RETURNING` (transactional statement).
- **Callback path** (`SaveCallbackAndApply`):
  1. `INSERT … ON CONFLICT (callback_id) DO NOTHING RETURNING id` — unique index is the race-free idempotency gate.
  2. Only if a row was inserted: `UPDATE documents … WHERE status = 'PROCESSING'`.
  3. Both steps share one transaction (Postgres) or one mutex section (memory).

Using `status = 'PROCESSING'` (not merely `status <> 'SUCCESS'`) also blocks illegal moves from PENDING or FAILED. Late FAILED after SUCCESS inserts the audit row (new `callback_id`) but the conditional update affects zero rows → `CallbackIgnored`.

---

## C. Recovery when Submit succeeded but the DB update failed

Window: `Principal.Submit` returned nil, then `MarkRequestAccepted` failed (crash, network blip to Postgres, canceled cleanup context).

**What is true**

- Document may still look PROCESSING (or briefly inconsistent if accept never stuck).
- Principal may already be working and will send a callback.
- We do **not** promise exactly-once delivery to Principal. The realistic guarantee is **at-least-once Submit with Principal-side idempotency** via `RequestID` / `Idempotency-Key`.

**Mitigations in this design**

1. **Request ID** so a later claim+Submit of the same attempt is deduplicated at Principal.
2. **Callback wins**: when the callback arrives, `SaveCallbackAndApply` moves PROCESSING → SUCCESS/FAILED regardless of whether `MarkRequestAccepted` ran (accept only refreshes `locked_at`).
3. **Lease recovery** (`locked_at` + sweeper): PROCESSING rows older than the lease are failed or released so work is not stuck forever if the callback never arrives.
4. **Outbox (not implemented, listed as a next improvement)**: write a “submit intent” in the same transaction as the claim; a separate sender publishes. That shrinks the window between durable state and the network call.

If Submit ran and the process died before any DB write after claim, the row stays PROCESSING with a lease; recovery or the callback closes the loop.

---

## D. EXPLAIN ANALYZE

**How to read a plan**

- **Seq Scan vs Index Scan**: Seq Scan walks the whole table; fine for tiny tables, suspicious when a selective filter (`batch_id = $1 AND status = 'PENDING'`) still seq-scans a large `documents` table → missing or unused index.
- **Estimated vs actual rows**: a large gap often means stale statistics → `ANALYZE documents;`.
- **`loops`**: nested-loop cost is per-iteration × loops; a cheap inner node × huge loops is expensive.
- **`Buffers: shared hit/read`**: hits are cache; high reads mean disk / cold cache.
- **Warning signs**: `Sort Method: external merge Disk`; Seq Scan + selective filter; repeated claims without using the partial PENDING indexes.

**Indexes proposed for this schema** (see `db/schema.sql`)

- I1 `(batch_id, status)` — counts and filtered lists.
- I2 partial `(id) WHERE status = 'PENDING'` — hot claim path.
- I3 partial `(batch_id, id) WHERE status IN ('PENDING','FAILED')` — per-batch claim / retry / unfinished checks.
- I4 unique `callbacks(callback_id)` — idempotency.
- I5 `callbacks(document_id)` — audit.
- I6 partial `(locked_at) WHERE status = 'PROCESSING'` — lease sweeper.

Trade-off: every status change updates indexes that include `status`. Partial indexes stay small because SUCCESS rows leave them.

**Safety**

`EXPLAIN ANALYZE` on `UPDATE`/`DELETE` **executes** the statement. Prefer:

```sql
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) /* your UPDATE or claim CTE */;
ROLLBACK;
```

on staging/replica, or plain `EXPLAIN` first. Verify claim and callback plans after realistic data volume, not on an empty database.
