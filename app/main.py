"""
RAG-Ops Observability Agent — FastAPI Application
==================================================
Phase 2: REST + SSE API surface.

Endpoints:
    POST /ingest          Trigger a CloudWatch ingestion run (background task).
    POST /query           Ask the agent a question; streams tokens via SSE.
    GET  /query/{id}      Retrieve a completed query with RAGAS scores.
    GET  /health          Liveness probe (used by Docker / K8s).
    GET  /metrics         Summary stats for the last 24 h of queries.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field

from app.agent import RagOpsAgent
from app.dependencies import get_agent, get_db_pool, lifespan_state
from app.schemas import (
    IngestRequest,
    IngestResponse,
    QueryRequest,
    QuerySummary,
    MetricsResponse,
)


# ---------------------------------------------------------------------------
# App lifespan — initialise shared resources once at startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start-up: warm the embedding model and DB pool. Shut-down: close them."""
    logger.info("RAG-Ops API starting up…")
    await lifespan_state.startup()
    yield
    logger.info("RAG-Ops API shutting down…")
    await lifespan_state.shutdown()


app = FastAPI(
    title="RAG-Ops Observability Agent",
    version="1.0.0",
    description="Query AWS CloudWatch errors and get grounded, evaluated fixes.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
async def health() -> dict:
    return {"status": "ok", "timestamp": datetime.now(tz=timezone.utc).isoformat()}


@app.post("/ingest", response_model=IngestResponse, tags=["ingestion"])
async def ingest(
    req: IngestRequest,
    background_tasks: BackgroundTasks,
    agent: RagOpsAgent = Depends(get_agent),
) -> IngestResponse:
    """
    Trigger a CloudWatch ingestion run in the background.
    Returns immediately with a run_id; poll /ingest/{run_id} for status.
    """
    run_id = str(uuid.uuid4())
    background_tasks.add_task(
        agent.run_ingestion,
        run_id=run_id,
        log_group=req.log_group,
        service_name=req.service_name,
        lookback_hours=req.lookback_hours,
        filter_pattern=req.filter_pattern,
    )
    logger.info("Ingestion task queued", run_id=run_id, log_group=req.log_group)
    return IngestResponse(run_id=run_id, status="queued")


@app.post("/query", tags=["query"])
async def query(
    req: QueryRequest,
    agent: RagOpsAgent = Depends(get_agent),
) -> StreamingResponse:
    """
    Query the RAG agent. Streams the response token-by-token via SSE.

    The client should set:
        Accept: text/event-stream
        Cache-Control: no-cache

    SSE event types emitted:
        data: {"type": "token",    "content": "..."}
        data: {"type": "sources",  "content": [...]}
        data: {"type": "scores",   "content": {...}}
        data: {"type": "done",     "query_id": "..."}
        data: {"type": "error",    "content": "..."}
    """
    query_id = str(uuid.uuid4())

    async def event_stream() -> AsyncGenerator[str, None]:
        try:
            async for event in agent.stream_query(
                query_id=query_id,
                question=req.question,
                start_ts=req.start_ts,
                end_ts=req.end_ts,
                log_level_filter=req.log_level_filter,
                service_filter=req.service_filter,
                top_k=req.top_k,
            ):
                yield f"data: {json.dumps(event)}\n\n"

        except asyncio.CancelledError:
            # Client disconnected; clean up gracefully
            logger.info("SSE client disconnected", query_id=query_id)
            return
        except Exception as exc:
            logger.exception("Stream error", query_id=query_id)
            yield f"data: {json.dumps({'type': 'error', 'content': str(exc)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",      # disable Nginx buffering for SSE
            "Connection": "keep-alive",
        },
    )


@app.get("/query/{query_id}", response_model=QuerySummary, tags=["query"])
async def get_query(
    query_id: str,
    agent: RagOpsAgent = Depends(get_agent),
) -> QuerySummary:
    """Retrieve a completed query with its RAGAS evaluation scores."""
    result = await agent.get_query(query_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Query {query_id!r} not found.")
    return result


@app.get("/metrics", response_model=MetricsResponse, tags=["ops"])
async def metrics(
    agent: RagOpsAgent = Depends(get_agent),
) -> MetricsResponse:
    """Return summary statistics for the last 24 hours of queries."""
    return await agent.get_metrics()
