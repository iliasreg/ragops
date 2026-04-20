"""
RAG-Ops — FastAPI dependency injection.

Holds singleton instances (DB pool, embedder, agent) initialised once at
startup and shared across all requests. Safe for async + multi-worker use
because oracledb pools are thread-safe and sentence-transformers models
are read-only after load.
"""

from __future__ import annotations

import os

import oracledb
from loguru import logger
from sentence_transformers import SentenceTransformer

from app.agent import RagOpsAgent


class _LifespanState:
    """Singleton state attached to the FastAPI app lifespan."""

    def __init__(self) -> None:
        self.pool: oracledb.AsyncConnectionPool | None = None
        self.embedder: SentenceTransformer | None = None
        self.agent: RagOpsAgent | None = None

    async def startup(self) -> None:
        logger.info("Initialising Oracle connection pool…")
        dsn = oracledb.makedsn(
            os.environ["ORACLE_HOST"],
            int(os.environ.get("ORACLE_PORT", "1521")),
            service_name=os.environ["ORACLE_SERVICE"],
        )
        self.pool = oracledb.create_pool_async(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASSWORD"],
            dsn=dsn,
            min=2,
            max=10,
            increment=1,
        )

        logger.info("Loading embedding model…")
        self.embedder = SentenceTransformer(
            os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        )

        self.agent = RagOpsAgent(pool=self.pool, embedder=self.embedder)
        logger.info("Startup complete.")

    async def shutdown(self) -> None:
        if self.pool:
            await self.pool.close()
        logger.info("Shutdown complete.")


lifespan_state = _LifespanState()


# ---------------------------------------------------------------------------
# FastAPI dependency functions
# ---------------------------------------------------------------------------

async def get_db_pool() -> oracledb.AsyncConnectionPool:
    assert lifespan_state.pool, "DB pool not initialised"
    return lifespan_state.pool


async def get_agent() -> RagOpsAgent:
    assert lifespan_state.agent, "Agent not initialised"
    return lifespan_state.agent
