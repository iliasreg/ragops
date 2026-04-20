"""
RAG-Ops — Pydantic v2 schemas for API request / response models.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    log_group: str = Field(..., examples=["/aws/lambda/my-service"])
    service_name: str = Field(..., examples=["my-service"])
    lookback_hours: int = Field(6, ge=1, le=168)        # max 1 week
    filter_pattern: str = Field("", examples=["?ERROR ?Exception"])


class IngestResponse(BaseModel):
    run_id: str
    status: str


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=2000)
    start_ts: Optional[datetime] = Field(
        default=None,
        description="Inclusive UTC start for log retrieval. Defaults to 24 h ago.",
    )
    end_ts: Optional[datetime] = Field(
        default=None,
        description="Inclusive UTC end. Defaults to now.",
    )
    log_level_filter: Optional[str] = Field(
        None, examples=["ERROR", "WARNING"]
    )
    service_filter: Optional[str] = Field(None, examples=["payment-service"])
    top_k: int = Field(8, ge=1, le=20)

    @model_validator(mode="after")
    def set_default_time_range(self) -> "QueryRequest":
        now = datetime.now(tz=timezone.utc)
        if self.end_ts is None:
            self.end_ts = now
        if self.start_ts is None:
            self.start_ts = now - timedelta(hours=24)
        return self


class SourceChunk(BaseModel):
    chunk_id: str
    service_name: str
    log_timestamp: datetime
    log_level: str
    request_id: str
    chunk_text: str
    score: float                # cosine distance (lower = more similar)
    is_complete: int
    completeness_note: str


class RagasScores(BaseModel):
    faithfulness: Optional[float] = None
    answer_relevancy: Optional[float] = None
    context_precision: Optional[float] = None


class QuerySummary(BaseModel):
    query_id: str
    question: str
    answer: Optional[str]
    sources: list[SourceChunk] = []
    ragas: RagasScores = Field(default_factory=RagasScores)
    had_incomplete_context: bool = False
    self_correction_applied: bool = False
    self_correction_note: Optional[str] = None
    created_at: datetime
    latency_ms: Optional[int] = None


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

class MetricsResponse(BaseModel):
    period_hours: int = 24
    total_queries: int
    mean_faithfulness: Optional[float]
    mean_answer_relevancy: Optional[float]
    low_faithfulness_count: int         # queries below 0.70 threshold
    incomplete_context_count: int
    self_correction_count: int
