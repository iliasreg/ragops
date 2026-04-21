CREATE TABLE log_chunks (
    chunk_id          VARCHAR2(64)   NOT NULL,
    log_group_name    VARCHAR2(512)  NOT NULL,
    log_stream_name   VARCHAR2(512),
    aws_account_id    VARCHAR2(16),
    aws_region        VARCHAR2(32),
    log_timestamp     TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at       TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    log_level         VARCHAR2(16),
    service_name      VARCHAR2(256),
    request_id        VARCHAR2(256),
    trace_id          VARCHAR2(256),
    chunk_text        CLOB           NOT NULL,
    chunk_seq         NUMBER(6)      DEFAULT 0,
    chunk_token_count NUMBER(6),
    is_complete       NUMBER(1)      DEFAULT 1,
    completeness_note VARCHAR2(1024),
    embedding         VECTOR(384, FLOAT32),
    source_type       VARCHAR2(32)   DEFAULT 'cloudwatch_log',
    CONSTRAINT pk_log_chunks PRIMARY KEY (chunk_id)
);
CREATE INDEX idx_log_chunks_time ON log_chunks (log_timestamp);
CREATE INDEX idx_log_chunks_level_svc ON log_chunks (log_level, service_name, log_timestamp);
CREATE INDEX idx_log_chunks_request ON log_chunks (request_id, chunk_seq);
CREATE INDEX idx_log_chunks_source ON log_chunks (source_type);
CREATE TABLE query_history (
    query_id          VARCHAR2(64)   NOT NULL,
    query_text        CLOB           NOT NULL,
    generated_answer  CLOB,
    retrieved_chunks  CLOB,
    ragas_faithfulness       NUMBER(5,4),
    ragas_answer_relevancy   NUMBER(5,4),
    ragas_context_precision  NUMBER(5,4),
    ragas_context_recall     NUMBER(5,4),
    had_incomplete_context   NUMBER(1) DEFAULT 0,
    self_correction_applied  NUMBER(1) DEFAULT 0,
    self_correction_note     VARCHAR2(2048),
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    evaluation_at     TIMESTAMP WITH TIME ZONE,
    latency_ms        NUMBER(10),
    CONSTRAINT pk_query_history PRIMARY KEY (query_id)
);
CREATE INDEX idx_qh_created ON query_history (created_at DESC);
CREATE INDEX idx_qh_faithfulness ON query_history (ragas_faithfulness);
CREATE TABLE ingestion_runs (
    run_id            VARCHAR2(64)   NOT NULL,
    log_group_name    VARCHAR2(512)  NOT NULL,
    start_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time          TIMESTAMP WITH TIME ZONE NOT NULL,
    chunks_ingested   NUMBER(10)     DEFAULT 0,
    chunks_skipped    NUMBER(10)     DEFAULT 0,
    status            VARCHAR2(16)   DEFAULT 'running',
    error_message     VARCHAR2(4000),
    run_at            TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_ingestion_runs PRIMARY KEY (run_id),
    CONSTRAINT chk_run_status CHECK (status IN ('running', 'success', 'failed'))
);
COMMIT;
EXIT;
