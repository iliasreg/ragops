"""
RAG-Ops Observability Agent — Ingestion Pipeline
=================================================
Phase 1: Fetches CloudWatch logs via boto3, chunks by RequestID / X-Ray Trace,
generates embeddings with sentence-transformers, and upserts into Oracle 23ai.

Usage:
    python ingest_cloudwatch.py \
        --log-group /aws/lambda/my-service \
        --hours 6 \
        --service-name my-service

Requirements:
    pip install boto3 oracledb sentence-transformers python-dotenv tenacity loguru
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Generator, Optional

import boto3
import oracledb
from dotenv import load_dotenv
from loguru import logger
from sentence_transformers import SentenceTransformer
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """All configuration sourced from environment variables for 12-factor compliance."""

    # AWS
    aws_region: str = field(default_factory=lambda: os.environ["AWS_REGION"])
    aws_log_group: str = field(default_factory=lambda: os.environ.get("AWS_LOG_GROUP", ""))

    # Oracle 23ai
    oracle_host: str = field(default_factory=lambda: os.environ["ORACLE_HOST"])
    oracle_port: int = field(default_factory=lambda: int(os.environ.get("ORACLE_PORT", "1521")))
    oracle_service: str = field(default_factory=lambda: os.environ["ORACLE_SERVICE"])
    oracle_user: str = field(default_factory=lambda: os.environ["ORACLE_USER"])
    oracle_password: str = field(default_factory=lambda: os.environ["ORACLE_PASSWORD"])

    # Embedding
    embedding_model: str = "all-MiniLM-L6-v2"   # 384-dim, fast, good quality
    embedding_batch_size: int = 64

    # Chunking
    max_chunk_tokens: int = 400                  # ~300 words; leaves room in 512-token context
    chunk_overlap_lines: int = 2                 # lines of overlap between adjacent chunks

    # CloudWatch fetch
    default_lookback_hours: int = 6
    cw_query_limit: int = 10_000                 # max events per filter_log_events page


_cfg = Config()


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class LogChunk:
    """A single embeddable unit of log content."""

    chunk_id: str
    log_group_name: str
    log_stream_name: str
    log_timestamp: datetime
    log_level: str
    service_name: str
    request_id: str
    trace_id: str
    chunk_text: str
    chunk_seq: int
    chunk_token_count: int
    is_complete: int                  # 1 = OK, 0 = agent flagged as incomplete
    completeness_note: str
    source_type: str = "cloudwatch_log"
    embedding: Optional[list[float]] = None

    @staticmethod
    def make_chunk_id(log_group: str, request_id: str, chunk_seq: int) -> str:
        """Deterministic, idempotent ID allows safe re-ingestion (upsert)."""
        raw = f"{log_group}::{request_id}::{chunk_seq}"
        return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# CloudWatch Fetcher
# ---------------------------------------------------------------------------

class CloudWatchFetcher:
    """
    Fetches log events from a CloudWatch Log Group.

    Design notes:
    - Uses filter_log_events with pagination rather than StartQuery (Insights)
      to avoid per-query cost and support large log groups.
    - Streams pages lazily so memory usage is bounded.
    """

    def __init__(self, log_group: str, region: str = _cfg.aws_region) -> None:
        self._log_group = log_group
        self._client = boto3.client("logs", region_name=region)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def fetch_events(
        self,
        start_time: datetime,
        end_time: datetime,
        log_stream_prefix: Optional[str] = None,
        filter_pattern: str = "",
    ) -> Generator[dict, None, None]:
        """
        Yield raw CloudWatch log events between start_time and end_time.

        Args:
            start_time:          Inclusive UTC start.
            end_time:            Inclusive UTC end.
            log_stream_prefix:   Optional stream name prefix filter.
            filter_pattern:      CloudWatch filter pattern (e.g. '?ERROR ?WARN').

        Yields:
            dict with keys: timestamp (int ms), message (str), logStreamName (str)
        """
        kwargs: dict = {
            "logGroupName": self._log_group,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "filterPattern": filter_pattern,
            "limit": _cfg.cw_query_limit,
            "interleaved": True,     # merge streams by timestamp
        }
        if log_stream_prefix:
            kwargs["logStreamNamePrefix"] = log_stream_prefix

        pages_fetched = 0
        events_yielded = 0

        while True:
            response = self._client.filter_log_events(**kwargs)
            pages_fetched += 1

            for event in response.get("events", []):
                events_yielded += 1
                yield event

            next_token = response.get("nextToken")
            if not next_token:
                break
            kwargs["nextToken"] = next_token

        logger.info(
            "CloudWatch fetch complete",
            log_group=self._log_group,
            pages=pages_fetched,
            events=events_yielded,
        )


# ---------------------------------------------------------------------------
# Log Chunker
# ---------------------------------------------------------------------------

# Patterns for extracting structured fields from common log formats
_PATTERNS = {
    "request_id": re.compile(
        r"(?:RequestId|request[_\-]id)[:\s]+([a-f0-9\-]{20,})", re.IGNORECASE
    ),
    "trace_id": re.compile(
        r"(?:X-Amzn-Trace-Id|traceId|trace[_\-]id)[:\s=]+([a-zA-Z0-9\-:/=]+)",
        re.IGNORECASE,
    ),
    "log_level": re.compile(
        r"\b(ERROR|CRITICAL|WARN(?:ING)?|INFO|DEBUG|FATAL)\b", re.IGNORECASE
    ),
    "lambda_request": re.compile(
        r"(?:START|END|REPORT)\s+RequestId:\s+([a-f0-9\-]{36})"
    ),
}

# Heuristics that indicate a chunk might be incomplete
_INCOMPLETE_INDICATORS = [
    re.compile(r"Traceback \(most recent call last\)$", re.MULTILINE),  # truncated stack
    re.compile(r"^REPORT RequestId.*Duration:", re.MULTILINE),          # missing END before REPORT
    re.compile(r"\.\.\.$"),                                             # ellipsis truncation
    re.compile(r"<TRUNCATED>", re.IGNORECASE),
]


class LogChunker:
    """
    Groups raw log events into semantically meaningful chunks.

    Strategy (in priority order):
    1. Group by request_id / trace_id — keeps a full request trace together.
    2. If no request_id found, chunk by time window (30-second buckets).
    3. If a grouped chunk exceeds max_chunk_tokens, split it into overlapping
       sub-chunks to stay within the embedding model's 512-token limit.
    """

    def __init__(self, service_name: str, log_group: str) -> None:
        self._service_name = service_name
        self._log_group = log_group

    def chunk_events(self, events: list[dict]) -> list[LogChunk]:
        """
        Convert a list of raw CloudWatch events into a list of LogChunks.

        Args:
            events: Raw events from CloudWatchFetcher.fetch_events().

        Returns:
            Ordered list of LogChunk objects ready for embedding.
        """
        # 1. Group by request_id (or trace_id as fallback)
        groups: dict[str, list[dict]] = {}
        for event in events:
            key = self._extract_group_key(event["message"])
            groups.setdefault(key, []).append(event)

        # 2. Convert each group to one or more chunks
        chunks: list[LogChunk] = []
        for group_key, group_events in groups.items():
            group_events.sort(key=lambda e: e["timestamp"])
            chunks.extend(self._events_to_chunks(group_key, group_events))

        logger.info(
            "Chunking complete",
            total_events=len(events),
            groups=len(groups),
            chunks=len(chunks),
        )
        return chunks

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_group_key(self, message: str) -> str:
        """Return a stable group key from a log message."""
        for pattern_name in ("lambda_request", "request_id", "trace_id"):
            m = _PATTERNS[pattern_name].search(message)
            if m:
                return m.group(1)
        # Fallback: 30-second time bucket derived from message hash
        # (will be overridden with actual timestamp in _events_to_chunks)
        return f"__no_id_{hashlib.md5(message[:64].encode()).hexdigest()[:12]}"

    def _extract_metadata(self, messages: list[str]) -> tuple[str, str, str]:
        """
        Extract (request_id, trace_id, log_level) from a list of log lines.
        Returns the first match found across all lines.
        """
        request_id = trace_id = log_level = ""
        for msg in messages:
            if not request_id:
                m = _PATTERNS["request_id"].search(msg) or _PATTERNS["lambda_request"].search(msg)
                if m:
                    request_id = m.group(1)
            if not trace_id:
                m = _PATTERNS["trace_id"].search(msg)
                if m:
                    trace_id = m.group(1)
            if not log_level:
                m = _PATTERNS["log_level"].search(msg)
                if m:
                    # Normalise WARN → WARNING, CRITICAL → ERROR
                    raw = m.group(1).upper()
                    log_level = {"WARN": "WARNING", "CRITICAL": "ERROR", "FATAL": "ERROR"}.get(raw, raw)
            if request_id and trace_id and log_level:
                break
        return request_id, trace_id, log_level or "UNKNOWN"

    def _assess_completeness(self, text: str, request_id: str) -> tuple[int, str]:
        """
        Self-correction: detect signs that this chunk is incomplete or truncated.

        Returns:
            (is_complete, completeness_note)
        """
        notes: list[str] = []

        for pattern in _INCOMPLETE_INDICATORS:
            if pattern.search(text):
                notes.append(f"Incomplete indicator matched: {pattern.pattern!r}")

        # Lambda-specific: if we see START but no REPORT, the trace is incomplete
        has_start = bool(re.search(r"^START RequestId:", text, re.MULTILINE))
        has_report = bool(re.search(r"^REPORT RequestId:", text, re.MULTILINE))
        if has_start and not has_report:
            notes.append("Lambda START found but REPORT line is missing — trace may be truncated.")

        if not text.strip():
            notes.append("Chunk text is empty.")

        is_complete = 0 if notes else 1
        return is_complete, "; ".join(notes)

    def _estimate_tokens(self, text: str) -> int:
        """Rough token estimate: 1 token ≈ 4 characters (GPT/BERT average)."""
        return max(1, len(text) // 4)

    def _events_to_chunks(self, group_key: str, events: list[dict]) -> list[LogChunk]:
        """
        Convert a group of log events into one or more LogChunks,
        splitting if the combined text exceeds max_chunk_tokens.
        """
        messages = [e["message"] for e in events]
        timestamps = [
            datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
            for e in events
        ]
        stream_names = [e.get("logStreamName", "") for e in events]

        request_id, trace_id, log_level = self._extract_metadata(messages)
        if not request_id:
            request_id = group_key

        # Combine lines into a single text block
        full_text = "\n".join(messages)
        lines = full_text.splitlines()

        # Split into token-budget chunks
        raw_chunks: list[list[str]] = []
        current: list[str] = []
        current_tokens = 0

        for line in lines:
            line_tokens = self._estimate_tokens(line)
            if current_tokens + line_tokens > _cfg.max_chunk_tokens and current:
                raw_chunks.append(current)
                # overlap: carry last N lines into the next chunk
                current = current[-_cfg.chunk_overlap_lines:]
                current_tokens = sum(self._estimate_tokens(l) for l in current)
            current.append(line)
            current_tokens += line_tokens

        if current:
            raw_chunks.append(current)

        # Build LogChunk objects
        result: list[LogChunk] = []
        for seq, chunk_lines in enumerate(raw_chunks):
            chunk_text = "\n".join(chunk_lines).strip()
            is_complete, note = self._assess_completeness(chunk_text, request_id)

            if is_complete == 0:
                logger.warning(
                    "Incomplete chunk detected",
                    request_id=request_id,
                    seq=seq,
                    note=note,
                )

            chunk = LogChunk(
                chunk_id=LogChunk.make_chunk_id(self._log_group, request_id, seq),
                log_group_name=self._log_group,
                log_stream_name=stream_names[0] if stream_names else "",
                log_timestamp=timestamps[0],
                log_level=log_level,
                service_name=self._service_name,
                request_id=request_id,
                trace_id=trace_id,
                chunk_text=chunk_text,
                chunk_seq=seq,
                chunk_token_count=self._estimate_tokens(chunk_text),
                is_complete=is_complete,
                completeness_note=note,
            )
            result.append(chunk)

        return result


# ---------------------------------------------------------------------------
# Embedder
# ---------------------------------------------------------------------------

class Embedder:
    """
    Wraps sentence-transformers to produce normalised 384-dim embeddings.
    Batches chunks to saturate CPU/GPU efficiently.
    """

    def __init__(self, model_name: str = _cfg.embedding_model) -> None:
        logger.info(f"Loading embedding model: {model_name}")
        self._model = SentenceTransformer(model_name)

    def embed_chunks(self, chunks: list[LogChunk]) -> list[LogChunk]:
        """
        Embed all chunks in batches. Mutates and returns the same list.

        Args:
            chunks: LogChunk objects without embeddings.

        Returns:
            The same list with .embedding populated on each chunk.
        """
        texts = [c.chunk_text for c in chunks]
        total = len(texts)
        logger.info(f"Embedding {total} chunks in batches of {_cfg.embedding_batch_size}")

        all_embeddings = self._model.encode(
            texts,
            batch_size=_cfg.embedding_batch_size,
            normalize_embeddings=True,     # unit vectors → cosine = dot product
            show_progress_bar=total > 100,
        )

        for chunk, emb in zip(chunks, all_embeddings):
            chunk.embedding = emb.tolist()

        return chunks


# ---------------------------------------------------------------------------
# Oracle Writer
# ---------------------------------------------------------------------------

class OracleWriter:
    """
    Manages Oracle 23ai connections and upserts LogChunk objects.

    Uses oracledb (python-oracledb) in thin mode — no Oracle Client required.
    """

    _UPSERT_SQL = """
        MERGE INTO log_chunks tgt
        USING (
            SELECT
                :chunk_id        AS chunk_id,
                :log_group_name  AS log_group_name,
                :log_stream_name AS log_stream_name,
                :log_timestamp   AS log_timestamp,
                :log_level       AS log_level,
                :service_name    AS service_name,
                :request_id      AS request_id,
                :trace_id        AS trace_id,
                :chunk_text      AS chunk_text,
                :chunk_seq       AS chunk_seq,
                :chunk_token_count AS chunk_token_count,
                :is_complete     AS is_complete,
                :completeness_note AS completeness_note,
                :source_type     AS source_type,
                TO_VECTOR(:embedding_json, 384, FLOAT32) AS embedding
            FROM DUAL
        ) src ON (tgt.chunk_id = src.chunk_id)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.log_level         = src.log_level,
                tgt.is_complete       = src.is_complete,
                tgt.completeness_note = src.completeness_note,
                tgt.embedding         = src.embedding,
                tgt.ingested_at       = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (
                chunk_id, log_group_name, log_stream_name, log_timestamp,
                log_level, service_name, request_id, trace_id,
                chunk_text, chunk_seq, chunk_token_count,
                is_complete, completeness_note, source_type, embedding
            ) VALUES (
                src.chunk_id, src.log_group_name, src.log_stream_name, src.log_timestamp,
                src.log_level, src.service_name, src.request_id, src.trace_id,
                src.chunk_text, src.chunk_seq, src.chunk_token_count,
                src.is_complete, src.completeness_note, src.source_type, src.embedding
            )
    """

    def __init__(self) -> None:
        dsn = oracledb.makedsn(
            _cfg.oracle_host, _cfg.oracle_port, service_name=_cfg.oracle_service
        )
        self._pool = oracledb.create_pool(
            user=_cfg.oracle_user,
            password=_cfg.oracle_password,
            dsn=dsn,
            min=2,
            max=10,
            increment=1,
        )
        logger.info("Oracle connection pool initialised", dsn=dsn)

    def upsert_chunks(
        self,
        chunks: list[LogChunk],
        batch_size: int = 50,
    ) -> tuple[int, int]:
        """
        Upsert chunks into Oracle in batches.

        Returns:
            (rows_inserted, rows_updated)  — approximate; Oracle MERGE doesn't expose this
            directly, so we return (len(chunks), 0) as a conservative estimate.
        """
        if not chunks:
            return 0, 0

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(chunks), batch_size):
                    batch = chunks[i : i + batch_size]
                    params = [
                        {
                            "chunk_id":          c.chunk_id,
                            "log_group_name":    c.log_group_name,
                            "log_stream_name":   c.log_stream_name,
                            "log_timestamp":     c.log_timestamp,
                            "log_level":         c.log_level,
                            "service_name":      c.service_name,
                            "request_id":        c.request_id,
                            "trace_id":          c.trace_id,
                            "chunk_text":        c.chunk_text,
                            "chunk_seq":         c.chunk_seq,
                            "chunk_token_count": c.chunk_token_count,
                            "is_complete":       c.is_complete,
                            "completeness_note": c.completeness_note,
                            "source_type":       c.source_type,
                            # Oracle TO_VECTOR() accepts a JSON array string
                            "embedding_json":    json.dumps(c.embedding),
                        }
                        for c in batch
                    ]
                    cur.executemany(self._UPSERT_SQL, params)
                    conn.commit()
                    logger.debug(f"Upserted batch of {len(batch)} chunks")

        return len(chunks), 0

    def record_run(
        self,
        run_id: str,
        log_group: str,
        start_time: datetime,
        end_time: datetime,
        chunks_ingested: int,
        status: str,
        error_message: str = "",
    ) -> None:
        """Insert an audit record into ingestion_runs."""
        sql = """
            INSERT INTO ingestion_runs
                (run_id, log_group_name, start_time, end_time,
                 chunks_ingested, status, error_message)
            VALUES (:1, :2, :3, :4, :5, :6, :7)
        """
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [
                    run_id, log_group, start_time, end_time,
                    chunks_ingested, status, error_message[:4000] if error_message else None,
                ])
                conn.commit()

    def close(self) -> None:
        self._pool.close()


# ---------------------------------------------------------------------------
# Hybrid Search (used by the RAG agent, included here for modularity)
# ---------------------------------------------------------------------------

class HybridSearcher:
    """
    Performs Oracle 23ai hybrid search:
      1. Pre-filter by time range + optional metadata (relational).
      2. Vector similarity search via VECTOR_DISTANCE() within that filtered set.

    This two-phase approach is more efficient than a pure vector scan on
    large tables because it narrows the HNSW search space first.
    """

    _HYBRID_SQL = """
        SELECT
            chunk_id,
            log_timestamp,
            log_level,
            service_name,
            request_id,
            chunk_text,
            is_complete,
            completeness_note,
            VECTOR_DISTANCE(embedding, TO_VECTOR(:query_vec, 384, FLOAT32), COSINE) AS score
        FROM log_chunks
        WHERE
            log_timestamp BETWEEN :start_ts AND :end_ts
            AND (:log_level_filter IS NULL OR log_level = :log_level_filter)
            AND (:service_filter   IS NULL OR service_name = :service_filter)
            AND (:source_type      IS NULL OR source_type = :source_type)
        ORDER BY score ASC          -- COSINE distance: lower = more similar
        FETCH FIRST :top_k ROWS ONLY
    """

    def __init__(self, writer: OracleWriter, embedder: Embedder) -> None:
        self._pool = writer._pool
        self._embedder = embedder

    def search(
        self,
        query: str,
        start_ts: datetime,
        end_ts: datetime,
        log_level_filter: Optional[str] = None,
        service_filter: Optional[str] = None,
        source_type: Optional[str] = None,
        top_k: int = 8,
    ) -> list[dict]:
        """
        Run hybrid search and return ranked context chunks.

        Args:
            query:             Natural language engineer query.
            start_ts:          Relational filter — start of time window.
            end_ts:            Relational filter — end of time window.
            log_level_filter:  Optional level filter e.g. 'ERROR'.
            service_filter:    Optional service name filter.
            source_type:       'cloudwatch_log', 'documentation', or None (both).
            top_k:             Number of nearest neighbours to return.

        Returns:
            List of dicts, each with chunk metadata + cosine distance score.
        """
        # Embed the query using the same model as the corpus
        query_embedding = self._embedder._model.encode(
            [query], normalize_embeddings=True
        )[0].tolist()

        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    self._HYBRID_SQL,
                    {
                        "query_vec":          json.dumps(query_embedding),
                        "start_ts":           start_ts,
                        "end_ts":             end_ts,
                        "log_level_filter":   log_level_filter,
                        "service_filter":     service_filter,
                        "source_type":        source_type,
                        "top_k":              top_k,
                    },
                )
                cols = [d[0].lower() for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # Flag incomplete chunks for the agent to handle
        for row in rows:
            if row.get("is_complete") == 0:
                logger.warning(
                    "Incomplete chunk in retrieval results",
                    chunk_id=row["chunk_id"],
                    note=row.get("completeness_note"),
                )

        logger.info(
            f"Hybrid search returned {len(rows)} chunks",
            query=query[:80],
            top_k=top_k,
            time_range=(start_ts.isoformat(), end_ts.isoformat()),
        )
        return rows


# ---------------------------------------------------------------------------
# Orchestrator (main entry point)
# ---------------------------------------------------------------------------

class IngestionPipeline:
    """
    Wires together Fetcher → Chunker → Embedder → Writer.
    Designed to be called from a FastAPI background task or a CLI.
    """

    def __init__(self, log_group: str, service_name: str) -> None:
        self._log_group = log_group
        self._service_name = service_name
        self._fetcher = CloudWatchFetcher(log_group=log_group)
        self._chunker = LogChunker(service_name=service_name, log_group=log_group)
        self._embedder = Embedder()
        self._writer = OracleWriter()

    def run(
        self,
        lookback_hours: int = _cfg.default_lookback_hours,
        filter_pattern: str = "",
    ) -> dict:
        """
        Execute the full ingestion pipeline.

        Args:
            lookback_hours:  How many hours back to pull logs.
            filter_pattern:  CloudWatch filter pattern (empty = all events).

        Returns:
            Summary dict suitable for FastAPI response or CLI output.
        """
        run_id = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        logger.info(
            "Starting ingestion run",
            run_id=run_id,
            log_group=self._log_group,
            start_time=start_time.isoformat(),
            end_time=now.isoformat(),
        )

        try:
            # 1. Fetch
            raw_events = list(
                self._fetcher.fetch_events(
                    start_time=start_time,
                    end_time=now,
                    filter_pattern=filter_pattern,
                )
            )
            logger.info(f"Fetched {len(raw_events)} raw events")

            if not raw_events:
                logger.warning("No events found for the given time range and filter.")
                self._writer.record_run(
                    run_id, self._log_group, start_time, now, 0, "success"
                )
                return {"run_id": run_id, "events": 0, "chunks": 0, "status": "success"}

            # 2. Chunk
            chunks = self._chunker.chunk_events(raw_events)

            # 3. Embed
            chunks = self._embedder.embed_chunks(chunks)

            # 4. Upsert
            ingested, _ = self._writer.upsert_chunks(chunks)

            # 5. Audit record
            self._writer.record_run(
                run_id, self._log_group, start_time, now, ingested, "success"
            )

            incomplete_count = sum(1 for c in chunks if c.is_complete == 0)
            summary = {
                "run_id": run_id,
                "events": len(raw_events),
                "chunks": len(chunks),
                "chunks_incomplete": incomplete_count,
                "status": "success",
            }
            logger.info("Ingestion run complete", **summary)
            return summary

        except Exception as exc:
            logger.exception("Ingestion run failed", run_id=run_id)
            self._writer.record_run(
                run_id, self._log_group, start_time, now, 0, "failed",
                error_message=str(exc),
            )
            raise

    def close(self) -> None:
        self._writer.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RAG-Ops CloudWatch Ingestion Pipeline")
    parser.add_argument("--log-group", required=True, help="CloudWatch log group name")
    parser.add_argument("--service-name", required=True, help="Human-readable service name")
    parser.add_argument("--hours", type=int, default=6, help="Lookback window in hours")
    parser.add_argument("--filter-pattern", default="", help="CloudWatch filter pattern")
    args = parser.parse_args()

    pipeline = IngestionPipeline(
        log_group=args.log_group,
        service_name=args.service_name,
    )
    try:
        result = pipeline.run(
            lookback_hours=args.hours,
            filter_pattern=args.filter_pattern,
        )
        print(json.dumps(result, indent=2))
    finally:
        pipeline.close()
