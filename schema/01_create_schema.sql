-- =============================================================================
-- RAG-Ops Observability Agent — Oracle 23ai Schema
-- Phase 1: Database Setup
--
-- Requires: Oracle 23ai (23.4+) with VECTOR data type support.
-- Run as: DBA or a user with CREATE TABLE, CREATE INDEX privileges.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. LOG CHUNKS TABLE
--    Stores chunked CloudWatch log entries with full relational metadata
--    and a 384-dimensional embedding vector (all-MiniLM-L6-v2 output).
-- -----------------------------------------------------------------------------
CREATE TABLE log_chunks (
    -- Primary key
    chunk_id          VARCHAR2(64)   NOT NULL,          -- SHA-256 of (log_group + request_id + chunk_seq)

    -- AWS / CloudWatch provenance
    log_group_name    VARCHAR2(512)  NOT NULL,           -- e.g. /aws/lambda/my-service
    log_stream_name   VARCHAR2(512),
    aws_account_id    VARCHAR2(16),
    aws_region        VARCHAR2(32),

    -- Relational metadata (used for pre-filtering before vector search)
    log_timestamp     TIMESTAMP WITH TIME ZONE NOT NULL, -- Original CloudWatch ingest time
    ingested_at       TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    log_level         VARCHAR2(16),                      -- ERROR, WARN, INFO, DEBUG
    service_name      VARCHAR2(256),                     -- Derived from log group or log fields
    request_id        VARCHAR2(256),                     -- X-Ray / AWS Request ID for grouping
    trace_id          VARCHAR2(256),                     -- AWS X-Ray trace ID

    -- Chunk content
    chunk_text        CLOB           NOT NULL,           -- Raw text of this chunk
    chunk_seq         NUMBER(6)      DEFAULT 0,          -- Position within the request trace
    chunk_token_count NUMBER(6),                         -- Approximate token count (for context budgeting)
    is_complete       NUMBER(1)      DEFAULT 1,          -- 0 = agent flagged this chunk as incomplete/truncated
    completeness_note VARCHAR2(1024),                    -- Reason if is_complete = 0

    -- Embedding (sentence-transformers all-MiniLM-L6-v2 → 384 dims, FLOAT32)
    embedding         VECTOR(384, FLOAT32),

    -- Source type: 'cloudwatch_log' or 'documentation'
    source_type       VARCHAR2(32)   DEFAULT 'cloudwatch_log',

    CONSTRAINT pk_log_chunks PRIMARY KEY (chunk_id)
)
-- Partition by month on log_timestamp for efficient time-range queries
PARTITION BY RANGE (log_timestamp) INTERVAL (INTERVAL '1' MONTH)
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00 UTC')
);

-- Comment on columns for self-documenting schema
COMMENT ON TABLE  log_chunks              IS 'Chunked log entries and documentation with vector embeddings for RAG retrieval.';
COMMENT ON COLUMN log_chunks.chunk_id    IS 'Deterministic SHA-256 hash of (log_group_name || request_id || chunk_seq). Enables idempotent upserts.';
COMMENT ON COLUMN log_chunks.embedding   IS '384-dim FLOAT32 vector from all-MiniLM-L6-v2. Used for cosine similarity search.';
COMMENT ON COLUMN log_chunks.is_complete IS '1=complete trace, 0=agent identified missing spans or truncated output.';


-- -----------------------------------------------------------------------------
-- 2. VECTOR INDEX (IVF — Inverted File with HNSW for <1M rows)
--    Enables sub-second approximate nearest-neighbour search.
--    COSINE distance matches the normalised embeddings from sentence-transformers.
-- -----------------------------------------------------------------------------
CREATE VECTOR INDEX idx_log_chunks_vector
    ON log_chunks (embedding)
    USING HNSW
    WITH TARGET ACCURACY 95                  -- trade recall vs speed; tune to 90 for very large sets
    DISTANCE COSINE
    PARAMETERS (M 16, EFCONSTRUCTION 100);  -- M=neighbours per layer, higher = better recall


-- -----------------------------------------------------------------------------
-- 3. RELATIONAL INDEXES (used in hybrid pre-filter WHERE clauses)
-- -----------------------------------------------------------------------------
CREATE INDEX idx_log_chunks_time
    ON log_chunks (log_timestamp)
    LOCAL;                                   -- LOCAL = per-partition, avoids cross-partition scans

CREATE INDEX idx_log_chunks_level_svc
    ON log_chunks (log_level, service_name, log_timestamp);

CREATE INDEX idx_log_chunks_request
    ON log_chunks (request_id, chunk_seq);

CREATE INDEX idx_log_chunks_source
    ON log_chunks (source_type);


-- -----------------------------------------------------------------------------
-- 4. QUERY HISTORY TABLE
--    Persists every engineer query and its RAGAS evaluation scores.
--    Used to build an offline evaluation dataset and to detect score regression.
-- -----------------------------------------------------------------------------
CREATE TABLE query_history (
    query_id          VARCHAR2(64)   NOT NULL,
    query_text        CLOB           NOT NULL,
    generated_answer  CLOB,
    retrieved_chunks  CLOB,                              -- JSON array of chunk_ids used as context

    -- RAGAS scores (NULL until evaluation job runs)
    ragas_faithfulness       NUMBER(5,4),                -- 0.0–1.0; <0.7 = hallucination risk
    ragas_answer_relevancy   NUMBER(5,4),
    ragas_context_precision  NUMBER(5,4),
    ragas_context_recall     NUMBER(5,4),

    -- Agent self-correction flags
    had_incomplete_context   NUMBER(1) DEFAULT 0,
    self_correction_applied  NUMBER(1) DEFAULT 0,
    self_correction_note     VARCHAR2(2048),

    -- Timing
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    evaluation_at     TIMESTAMP WITH TIME ZONE,
    latency_ms        NUMBER(10),

    CONSTRAINT pk_query_history PRIMARY KEY (query_id)
);

CREATE INDEX idx_qh_created ON query_history (created_at DESC);
CREATE INDEX idx_qh_faithfulness ON query_history (ragas_faithfulness);

COMMENT ON TABLE  query_history                     IS 'Audit log for all engineer queries with RAGAS evaluation scores.';
COMMENT ON COLUMN query_history.retrieved_chunks    IS 'JSON array: [{"chunk_id":"...", "score":0.92, "source_type":"cloudwatch_log"}]';
COMMENT ON COLUMN query_history.ragas_faithfulness  IS 'RAGAS faithfulness: fraction of answer claims grounded in retrieved context. Alert if < 0.7.';


-- -----------------------------------------------------------------------------
-- 5. INGESTION RUNS TABLE
--    Tracks each boto3 ingestion run for observability and deduplication.
-- -----------------------------------------------------------------------------
CREATE TABLE ingestion_runs (
    run_id            VARCHAR2(64)   NOT NULL,
    log_group_name    VARCHAR2(512)  NOT NULL,
    start_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time          TIMESTAMP WITH TIME ZONE NOT NULL,
    chunks_ingested   NUMBER(10)     DEFAULT 0,
    chunks_skipped    NUMBER(10)     DEFAULT 0,   -- already-existing chunk_ids
    status            VARCHAR2(16)   DEFAULT 'running',  -- running | success | failed
    error_message     VARCHAR2(4000),
    run_at            TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_ingestion_runs PRIMARY KEY (run_id),
    CONSTRAINT chk_run_status CHECK (status IN ('running', 'success', 'failed'))
);

COMMENT ON TABLE ingestion_runs IS 'Audit trail for each CloudWatch ingestion job run.';
