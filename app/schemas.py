"""
RAG-Ops — Pydantic v2 schemas for API request / response models.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


class IngestRequest(BaseModel):
    log_group: str = Field(..., examples=["/aws/lambda/my-service"])
    service_name: str = Field(..., examples=["my-service"])
    lookback_hours: int = Field(6, ge=1, le=168)  # max 1 week
    filter_pattern: str = Field("", examples=["?ERROR ?Exception"])


class IngestResponse(BaseModel):
    run_id: str
    status: str


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=2000)
    start_ts: datetime | None = Field(
        default=None,
        description="Inclusive UTC start for log retrieval. Defaults to 24 h ago.",
    )
    end_ts: datetime | None = Field(
        default=None,
        description="Inclusive UTC end. Defaults to now.",
    )
    log_level_filter: str | None = Field(None, examples=["ERROR", "WARNING"])
    service_filter: str | None = Field(None, examples=["payment-service"])
    top_k: int = Field(8, ge=1, le=20)

    @model_validator(mode="after")
    def set_default_time_range(self) -> QueryRequest:
        now = datetime.now(tz=UTC)
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
    score: float  # cosine distance (lower = more similar)
    is_complete: int
    completeness_note: str


class RagasScores(BaseModel):
    faithfulness: float | None = None
    answer_relevancy: float | None = None
    context_precision: float | None = None


class QuerySummary(BaseModel):
    query_id: str
    question: str
    answer: str | None
    sources: list[SourceChunk] = []
    ragas: RagasScores = Field(default_factory=RagasScores)
    had_incomplete_context: bool = False
    self_correction_applied: bool = False
    self_correction_note: str | None = None
    created_at: datetime
    latency_ms: int | None = None


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class MetricsResponse(BaseModel):
    period_hours: int = 24
    total_queries: int
    mean_faithfulness: float | None
    mean_answer_relevancy: float | None
    low_faithfulness_count: int  # queries below 0.70 threshold
    incomplete_context_count: int
    self_correction_count: int
