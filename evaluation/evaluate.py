"""
RAG-Ops Observability Agent — RAGAS Evaluation Script
======================================================
Phase 1: Faithfulness evaluation to detect hallucinated fixes.

This script:
1. Loads recent query history from Oracle.
2. Evaluates each (query, answer, context) triple with RAGAS.
3. Writes scores back to query_history and triggers alerts on low faithfulness.

Usage:
    python evaluate.py --min-faithfulness 0.70 --lookback-hours 24

Requirements:
    pip install ragas datasets langchain-openai oracledb loguru
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import oracledb
from datasets import Dataset
from loguru import logger
from ragas import evaluate
from ragas.metrics import (
    answer_relevancy,
    context_precision,
    faithfulness,
)

# ---------------------------------------------------------------------------
# Faithfulness threshold — alert if below this value
# ---------------------------------------------------------------------------
FAITHFULNESS_ALERT_THRESHOLD = float(os.environ.get("FAITHFULNESS_THRESHOLD", "0.70"))


# ---------------------------------------------------------------------------
# Load evaluation samples from Oracle
# ---------------------------------------------------------------------------

def load_evaluation_samples(
    conn: oracledb.Connection,
    lookback_hours: int = 24,
    limit: int = 100,
) -> list[dict]:
    """
    Fetch unevaluated queries from query_history.

    Returns a list of dicts with keys required by RAGAS:
        question, answer, contexts, ground_truth (optional)
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=lookback_hours)

    sql = """
        SELECT query_id, query_text, generated_answer, retrieved_chunks
        FROM query_history
        WHERE created_at >= :cutoff
          AND ragas_faithfulness IS NULL    -- only unevaluated rows
          AND generated_answer IS NOT NULL
        ORDER BY created_at DESC
        FETCH FIRST :lim ROWS ONLY
    """

    with conn.cursor() as cur:
        cur.execute(sql, {"cutoff": cutoff, "lim": limit})
        rows = cur.fetchall()

    samples = []
    for query_id, question, answer, chunks_json in rows:
        try:
            chunk_list = json.loads(chunks_json or "[]")
            # RAGAS expects contexts as a list of strings
            contexts = [c.get("chunk_text", "") for c in chunk_list if c.get("chunk_text")]
        except (json.JSONDecodeError, TypeError):
            contexts = []

        samples.append({
            "_query_id": query_id,          # internal; not passed to RAGAS
            "question":  question,
            "answer":    answer,
            "contexts":  contexts,
            # ground_truth is optional; supply it if you have golden answers
            # "ground_truth": "...",
        })

    logger.info(f"Loaded {len(samples)} unevaluated queries from Oracle")
    return samples


# ---------------------------------------------------------------------------
# Run RAGAS evaluation
# ---------------------------------------------------------------------------

def run_ragas_evaluation(samples: list[dict]) -> list[dict]:
    """
    Evaluate samples with RAGAS Faithfulness, Answer Relevancy,
    and Context Precision.

    RAGAS Faithfulness specifically measures whether each claim in the
    generated answer is entailed by the retrieved context — this is the
    primary guard against hallucinated fixes.

    Args:
        samples: List of dicts with question/answer/contexts keys.

    Returns:
        Same list, each dict augmented with RAGAS score fields.
    """
    if not samples:
        return []

    # Strip internal keys before passing to RAGAS
    ragas_samples = [
        {k: v for k, v in s.items() if not k.startswith("_")}
        for s in samples
    ]

    dataset = Dataset.from_list(ragas_samples)

    logger.info(f"Running RAGAS evaluation on {len(samples)} samples...")
    results = evaluate(
        dataset=dataset,
        metrics=[
            faithfulness,         # PRIMARY: are answer claims grounded in context?
            answer_relevancy,     # Does the answer address the question?
            context_precision,    # Are the retrieved chunks actually relevant?
        ],
    )

    scores_df = results.to_pandas()

    # Merge scores back onto original samples (preserving _query_id)
    enriched = []
    for i, sample in enumerate(samples):
        row = scores_df.iloc[i]
        sample["ragas_faithfulness"]      = float(row.get("faithfulness",      0.0))
        sample["ragas_answer_relevancy"]  = float(row.get("answer_relevancy",  0.0))
        sample["ragas_context_precision"] = float(row.get("context_precision", 0.0))
        enriched.append(sample)

    logger.info(
        "RAGAS evaluation complete",
        mean_faithfulness=scores_df["faithfulness"].mean(),
        mean_relevancy=scores_df.get("answer_relevancy", scores_df).mean() if "answer_relevancy" in scores_df else "N/A",
    )
    return enriched


# ---------------------------------------------------------------------------
# Write scores back to Oracle + alert on low faithfulness
# ---------------------------------------------------------------------------

def persist_scores(conn: oracledb.Connection, evaluated: list[dict]) -> None:
    """
    Write RAGAS scores back to query_history.
    Logs a WARNING for any row where faithfulness < threshold.
    """
    update_sql = """
        UPDATE query_history
        SET
            ragas_faithfulness      = :faith,
            ragas_answer_relevancy  = :relevancy,
            ragas_context_precision = :precision,
            evaluation_at           = SYSTIMESTAMP
        WHERE query_id = :qid
    """

    params = []
    for s in evaluated:
        faith = s["ragas_faithfulness"]
        if faith < FAITHFULNESS_ALERT_THRESHOLD:
            logger.warning(
                "LOW FAITHFULNESS — potential hallucination",
                query_id=s["_query_id"],
                faithfulness=round(faith, 4),
                question=s["question"][:120],
                threshold=FAITHFULNESS_ALERT_THRESHOLD,
            )
        params.append({
            "faith":     faith,
            "relevancy": s["ragas_answer_relevancy"],
            "precision": s["ragas_context_precision"],
            "qid":       s["_query_id"],
        })

    with conn.cursor() as cur:
        cur.executemany(update_sql, params)
        conn.commit()

    logger.info(f"Persisted scores for {len(params)} queries")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RAG-Ops RAGAS Evaluation Job")
    parser.add_argument("--lookback-hours",    type=int,   default=24)
    parser.add_argument("--limit",             type=int,   default=100)
    parser.add_argument("--min-faithfulness",  type=float, default=0.70)
    args = parser.parse_args()

    FAITHFULNESS_ALERT_THRESHOLD = args.min_faithfulness

    dsn = oracledb.makedsn(
        os.environ["ORACLE_HOST"],
        int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=os.environ["ORACLE_SERVICE"],
    )
    conn = oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=dsn,
    )

    try:
        samples   = load_evaluation_samples(conn, lookback_hours=args.lookback_hours, limit=args.limit)
        evaluated = run_ragas_evaluation(samples)
        persist_scores(conn, evaluated)
    finally:
        conn.close()
