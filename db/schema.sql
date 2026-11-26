-- PostgreSQL schema for the bulk document processing service.
-- Design decisions D1..D7 are explained in README.md and docs/answers.md.

CREATE TABLE batches (
    id          BIGSERIAL   PRIMARY KEY,
    -- D6: unique batch number, so a client can safely retry CreateBatch.
    batch_no    TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE documents (
    id            BIGSERIAL   PRIMARY KEY,
    batch_id      BIGINT      NOT NULL REFERENCES batches (id),
    external_ref  TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED')),
    attempt_count INT         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    -- D3: the retry budget is data, so a policy change needs no deploy and
    -- existing rows keep the limit they were created with.
    max_attempts  INT         NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    -- D5: lease marker; set while PROCESSING, used to recover stuck documents.
    locked_at     TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    last_error    TEXT,
    payload       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- D1: no duplicate document inside a batch.
    UNIQUE (batch_id, external_ref),
    -- D4: invariants live in the database, so a buggy service cannot break them.
    CHECK (attempt_count <= max_attempts),
    CHECK (status <> 'PROCESSING' OR locked_at IS NOT NULL)
);

CREATE TABLE callbacks (
    id           BIGSERIAL   PRIMARY KEY,
    callback_id  TEXT        NOT NULL,
    document_id  BIGINT      NOT NULL REFERENCES documents (id),
    result       TEXT        NOT NULL CHECK (result IN ('SUCCESS', 'FAILED')),
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- D7: audit trail for debugging.
    raw_body     JSONB
);

-- D2 / I4: the idempotency key. Only a unique index is race-free; a
-- check-then-insert in application code is not.
CREATE UNIQUE INDEX ux_callbacks_callback_id ON callbacks (callback_id);

-- Indexes (proposal I1..I5; see docs/answers.md for the trade-offs).

-- I1: per-batch counts and "WHERE batch_id = $1 AND status = $2" lookups.
CREATE INDEX ix_documents_batch_status ON documents (batch_id, status);

-- I2 (partial): small, hot index for claiming PENDING work without a batch filter.
CREATE INDEX ix_documents_pending ON documents (id) WHERE status = 'PENDING';

-- I3 (partial): claim within one batch, RetryFailed and "not finished" checks.
-- status = 'PENDING' implies the index predicate, so the claim query can use it too.
CREATE INDEX ix_documents_batch_open ON documents (batch_id, id) WHERE status IN ('PENDING', 'FAILED');

-- I5 (optional): audit lookup of callbacks by document.
CREATE INDEX ix_callbacks_document ON callbacks (document_id);

-- I6 (partial): lease recovery scans only PROCESSING rows by age.
CREATE INDEX ix_documents_processing_lease ON documents (locked_at) WHERE status = 'PROCESSING';
