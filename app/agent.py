"""
RAG-Ops Observability Agent — LangChain Agent Core
===================================================
Phase 2: Streaming RAG agent with self-correction loop.

Flow per query:
  1. Hybrid search (relational pre-filter + VECTOR_DISTANCE).
  2. Self-correction check — if context is incomplete, issue a second
     broader search before generating.
  3. Stream LLM tokens via SSE.
  4. Persist query + retrieved chunk IDs to query_history.
  5. (Async) Run RAGAS faithfulness evaluation; update scores.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
import os
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Optional

import oracledb
from langchain.callbacks.streaming_aiter import AsyncIteratorCallbackHandler
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import HumanMessage, SystemMessage
from loguru import logger
from sentence_transformers import SentenceTransformer

from app.schemas import MetricsResponse, QuerySummary, RagasScores, SourceChunk
from ingestion.ingest_cloudwatch import IngestionPipeline


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are RAG-Ops, an expert SRE assistant specialising in diagnosing AWS \
CloudWatch errors and suggesting production fixes.

Rules:
1. Base EVERY fix suggestion exclusively on the log context provided below.
2. If the context is insufficient, say so explicitly — do NOT invent details.
3. Format your response as:
   ## Root Cause
   ## Evidence (quote the relevant log lines)
   ## Suggested Fix
   ## Confidence  (High / Medium / Low — based on context completeness)
4. If any context chunk is marked INCOMPLETE, lower your confidence and flag it.

--- RETRIEVED CONTEXT ---
{context}
--- END CONTEXT ---
"""

_INCOMPLETE_PREFIX = (
    "\n⚠️  NOTE: One or more retrieved log chunks were flagged as INCOMPLETE. "
    "The trace may be truncated. Treat suggestions as Medium confidence at most.\n"
)


# ---------------------------------------------------------------------------
# Hybrid search SQL (async version)
# ---------------------------------------------------------------------------

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
    ORDER BY score ASC
    FETCH FIRST :top_k ROWS ONLY
"""

_BROADER_SQL = """
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
    WHERE log_timestamp BETWEEN :start_ts AND :end_ts
    ORDER BY score ASC
    FETCH FIRST :top_k ROWS ONLY
"""


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class RagOpsAgent:
    """
    Orchestrates retrieval, self-correction, LLM generation, and evaluation.
    Designed to be instantiated once and reused across requests.
    """

    def __init__(
        self,
        pool: oracledb.AsyncConnectionPool,
        embedder: SentenceTransformer,
        llm_model: str = "gpt-4o",
        temperature: float = 0.1,
    ) -> None:
        self._pool = pool
        self._embedder = embedder
        self._llm_model = llm_model
        self._temperature = temperature

    # ------------------------------------------------------------------
    # Public: streaming query
    # ------------------------------------------------------------------

    async def stream_query(
        self,
        query_id: str,
        question: str,
        start_ts: datetime,
        end_ts: datetime,
        log_level_filter: Optional[str] = None,
        service_filter: Optional[str] = None,
        top_k: int = 8,
    ) -> AsyncGenerator[dict, None]:
        """
        Main entry point for the /query endpoint.

        Yields SSE event dicts:
            {"type": "token",   "content": "<token>"}
            {"type": "sources", "content": [SourceChunk dicts]}
            {"type": "scores",  "content": {faithfulness, ...}}
            {"type": "done",    "query_id": "..."}
        """
        t_start = time.monotonic()

        # 1. Embed the question
        query_vec = self._embed(question)

        # 2. Hybrid search
        chunks = await self._hybrid_search(
            query_vec=query_vec,
            start_ts=start_ts,
            end_ts=end_ts,
            log_level_filter=log_level_filter,
            service_filter=service_filter,
            top_k=top_k,
        )

        # 3. Self-correction: if all top results are incomplete, broaden search
        self_correction_applied = False
        self_correction_note = ""
        had_incomplete = any(c["is_complete"] == 0 for c in chunks)

        if had_incomplete and all(c["is_complete"] == 0 for c in chunks):
            logger.warning(
                "All retrieved chunks incomplete — applying self-correction",
                query_id=query_id,
            )
            broader = await self._broader_search(
                query_vec=query_vec,
                start_ts=start_ts - timedelta(hours=2),   # widen time window
                end_ts=end_ts,
                top_k=top_k,
            )
            # Merge: prefer complete chunks from broader search
            seen = {c["chunk_id"] for c in chunks}
            for bc in broader:
                if bc["chunk_id"] not in seen and bc["is_complete"] == 1:
                    chunks.append(bc)
                    seen.add(bc["chunk_id"])
            self_correction_applied = True
            self_correction_note = (
                "All initial chunks were incomplete. Broadened time window by 2 h "
                "and merged complete chunks from a second search pass."
            )
            logger.info("Self-correction complete", query_id=query_id, total_chunks=len(chunks))

        # 4. Build context string
        context_parts: list[str] = []
        for c in chunks:
            header = (
                f"[{c['service_name']} | {c['log_level']} | "
                f"{c['log_timestamp']} | score={c['score']:.3f}"
            )
            if c["is_complete"] == 0:
                header += f" | ⚠ INCOMPLETE: {c['completeness_note']}"
            context_parts.append(f"{header}]\n{c['chunk_text']}")

        context = "\n\n---\n\n".join(context_parts)
        if had_incomplete:
            context = _INCOMPLETE_PREFIX + context

        # 5. Stream LLM response
        callback = AsyncIteratorCallbackHandler()
        llm = ChatOpenAI(
            model=os.environ.get("OPENROUTER_MODEL", "llama-3.3-70b-versatile"),
            temperature=self._temperature,
            streaming=True,
            callbacks=[callback],
            openai_api_key=os.environ["OPENROUTER_API_KEY"],
            openai_api_base="https://api.groq.com/openai/v1",
            default_headers={
                "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            },
        )

        full_answer_parts: list[str] = []

        async def _run_llm() -> None:
            prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content=_SYSTEM_PROMPT.format(context=context)),
                HumanMessage(content=question),
            ])
            chain = prompt | llm
            await chain.ainvoke({})

        llm_task = asyncio.create_task(_run_llm())

        async for token in callback.aiter():
            full_answer_parts.append(token)
            yield {"type": "token", "content": token}

        await llm_task

        full_answer = "".join(full_answer_parts)
        latency_ms = int((time.monotonic() - t_start) * 1000)

        # 6. Emit sources
        source_dicts = [self._row_to_source(c) for c in chunks]
        yield {"type": "sources", "content": source_dicts}

        # 7. Persist to query_history
        await self._save_query(
            query_id=query_id,
            question=question,
            answer=full_answer,
            chunks=chunks,
            had_incomplete=had_incomplete,
            self_correction_applied=self_correction_applied,
            self_correction_note=self_correction_note,
            latency_ms=latency_ms,
        )

        # 8. Async RAGAS evaluation (fire-and-forget; scores written back later)
        asyncio.create_task(
            self._evaluate_and_update(
                query_id=query_id,
                question=question,
                answer=full_answer,
                chunks=chunks,
            )
        )

        yield {"type": "done", "query_id": query_id}

    # ------------------------------------------------------------------
    # Public: ingestion (called as a background task)
    # ------------------------------------------------------------------

    async def run_ingestion(
        self,
        run_id: str,
        log_group: str,
        service_name: str,
        lookback_hours: int,
        filter_pattern: str,
    ) -> None:
        """Runs the ingestion pipeline in a thread pool (CPU-bound work)."""
        loop = asyncio.get_event_loop()
        pipeline = IngestionPipeline(log_group=log_group, service_name=service_name)
        try:
            result = await loop.run_in_executor(
                None,
                lambda: pipeline.run(
                    lookback_hours=lookback_hours,
                    filter_pattern=filter_pattern,
                ),
            )
            logger.info("Background ingestion complete", run_id=run_id, **result)
        finally:
            pipeline.close()

    # ------------------------------------------------------------------
    # Public: query history + metrics
    # ------------------------------------------------------------------

    async def get_query(self, query_id: str) -> Optional[QuerySummary]:
        sql = """
            SELECT query_id, query_text, generated_answer, retrieved_chunks,
                   ragas_faithfulness, ragas_answer_relevancy, ragas_context_precision,
                   had_incomplete_context, self_correction_applied, self_correction_note,
                   created_at, latency_ms
            FROM query_history WHERE query_id = :qid
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, {"qid": query_id})
                row = await cur.fetchone()

        if not row:
            return None

        (qid, question, answer, chunks_json, faith, rel, prec,
         incomplete, correction, note, created_at, latency) = row

        chunks = json.loads(chunks_json or "[]")
        return QuerySummary(
            query_id=qid,
            question=question,
            answer=answer,
            sources=[SourceChunk(**c) for c in chunks],
            ragas=RagasScores(
                faithfulness=faith,
                answer_relevancy=rel,
                context_precision=prec,
            ),
            had_incomplete_context=bool(incomplete),
            self_correction_applied=bool(correction),
            self_correction_note=note,
            created_at=created_at,
            latency_ms=latency,
        )

    async def get_metrics(self) -> MetricsResponse:
        sql = """
            SELECT
                COUNT(*)                                          AS total,
                AVG(ragas_faithfulness)                          AS mean_faith,
                AVG(ragas_answer_relevancy)                      AS mean_rel,
                SUM(CASE WHEN ragas_faithfulness < 0.7 THEN 1 ELSE 0 END) AS low_faith,
                SUM(had_incomplete_context)                      AS incomplete,
                SUM(self_correction_applied)                     AS corrections
            FROM query_history
            WHERE created_at >= :cutoff
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, {"cutoff": cutoff})
                row = await cur.fetchone()

        total, faith, rel, low, inc, corr = row or (0, None, None, 0, 0, 0)
        return MetricsResponse(
            total_queries=total or 0,
            mean_faithfulness=round(faith, 4) if faith else None,
            mean_answer_relevancy=round(rel, 4) if rel else None,
            low_faithfulness_count=low or 0,
            incomplete_context_count=inc or 0,
            self_correction_count=corr or 0,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _embed(self, text: str) -> list[float]:
        return self._embedder.encode(
            [text], normalize_embeddings=True
        )[0].tolist()

    async def _hybrid_search(
        self,
        query_vec: list[float],
        start_ts: datetime,
        end_ts: datetime,
        log_level_filter: Optional[str],
        service_filter: Optional[str],
        top_k: int,
    ) -> list[dict]:
        return await self._run_search(
            sql=_HYBRID_SQL,
            params={
                "query_vec":        json.dumps(query_vec),
                "start_ts":         start_ts,
                "end_ts":           end_ts,
                "log_level_filter": log_level_filter,
                "service_filter":   service_filter,
                "top_k":            top_k,
            },
        )

    async def _broader_search(
        self,
        query_vec: list[float],
        start_ts: datetime,
        end_ts: datetime,
        top_k: int,
    ) -> list[dict]:
        return await self._run_search(
            sql=_BROADER_SQL,
            params={
                "query_vec": json.dumps(query_vec),
                "start_ts":  start_ts,
                "end_ts":    end_ts,
                "top_k":     top_k,
            },
        )

    async def _run_search(self, sql: str, params: dict) -> list[dict]:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                cols = [d[0].lower() for d in cur.description]
                rows = await cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]

    @staticmethod
    def _row_to_source(row: dict) -> dict:
        return {
            "chunk_id":          row["chunk_id"],
            "service_name":      row.get("service_name", ""),
            "log_timestamp":     row["log_timestamp"].isoformat()
                                 if hasattr(row["log_timestamp"], "isoformat")
                                 else str(row["log_timestamp"]),
            "log_level":         row.get("log_level", ""),
            "request_id":        row.get("request_id", ""),
            "chunk_text":        row["chunk_text"],
            "score":             float(row["score"]),
            "is_complete":       int(row.get("is_complete", 1)),
            "completeness_note": row.get("completeness_note", "") or "",
        }

    async def _save_query(
        self,
        query_id: str,
        question: str,
        answer: str,
        chunks: list[dict],
        had_incomplete: bool,
        self_correction_applied: bool,
        self_correction_note: str,
        latency_ms: int,
    ) -> None:
        chunks_json = json.dumps([self._row_to_source(c) for c in chunks])
        sql = """
            INSERT INTO query_history (
                query_id, query_text, generated_answer, retrieved_chunks,
                had_incomplete_context, self_correction_applied, self_correction_note,
                latency_ms, created_at
            ) VALUES (
                :qid, :question, :answer, :chunks,
                :incomplete, :correction, :note,
                :latency, SYSTIMESTAMP
            )
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, {
                    "qid":        query_id,
                    "question":   question,
                    "answer":     answer,
                    "chunks":     chunks_json,
                    "incomplete": int(had_incomplete),
                    "correction": int(self_correction_applied),
                    "note":       self_correction_note or None,
                    "latency":    latency_ms,
                })
                await conn.commit()

    async def _evaluate_and_update(
        self,
        query_id: str,
        question: str,
        answer: str,
        chunks: list[dict],
    ) -> None:
        """
        Run RAGAS faithfulness evaluation asynchronously.
        Writes scores back to query_history when complete.
        """
        try:
            from datasets import Dataset
            from ragas import evaluate as ragas_evaluate
            from ragas.metrics import answer_relevancy, context_precision, faithfulness

            contexts = [c["chunk_text"] for c in chunks if c.get("chunk_text")]
            if not contexts or not answer:
                return

            dataset = Dataset.from_list([{
                "question": question,
                "answer":   answer,
                "contexts": contexts,
            }])

            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                lambda: ragas_evaluate(
                    dataset=dataset,
                    metrics=[faithfulness, answer_relevancy, context_precision],
                ),
            )
            df = results.to_pandas()
            faith = float(df["faithfulness"].iloc[0])
            rel   = float(df.get("answer_relevancy", df).iloc[0]) if "answer_relevancy" in df else None
            prec  = float(df.get("context_precision", df).iloc[0]) if "context_precision" in df else None

            if faith < 0.70:
                logger.warning(
                    "LOW FAITHFULNESS — potential hallucination",
                    query_id=query_id,
                    faithfulness=round(faith, 4),
                )

            update_sql = """
                UPDATE query_history
                SET ragas_faithfulness      = :faith,
                    ragas_answer_relevancy  = :rel,
                    ragas_context_precision = :prec,
                    evaluation_at           = SYSTIMESTAMP
                WHERE query_id = :qid
            """
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(update_sql, {
                        "faith": faith, "rel": rel, "prec": prec, "qid": query_id,
                    })
                    await conn.commit()

            logger.info("RAGAS evaluation complete", query_id=query_id, faithfulness=round(faith, 4))

        except Exception:
            logger.exception("RAGAS evaluation failed (non-fatal)", query_id=query_id)
