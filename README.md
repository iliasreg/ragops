# RAG-Ops

RAG-based diagnostic agent that autonomously analyzes AWS CloudWatch logs and
returns grounded, evaluated fixes for production errors.

Given a natural-language question about a CloudWatch log group, RAG-Ops:

1. Retrieves the most relevant log chunks with hybrid search (relational
   pre-filter + Oracle 23ai `VECTOR_DISTANCE`).
2. Self-corrects — issues a broader second search if the context looks
   incomplete.
3. Streams an LLM answer token-by-token (SSE) grounded strictly in the
   retrieved context.
4. Persists the query and evaluates it with RAGAS (faithfulness, answer
   relevancy, context precision).

## Architecture

| Directory      | Purpose                                                         |
|----------------|-----------------------------------------------------------------|
| `app/`         | FastAPI application: agent core, dependencies, API + SSE routes |
| `ingestion/`   | CloudWatch log ingestion pipeline (chunking + embedding)        |
| `evaluation/`  | RAGAS evaluation harness                                        |
| `schema/`      | Oracle 23ai DDL (vector-indexed `log_chunks`, query history)     |
| `frontend/`    | React single-page app (query, streamed answer, sources, metrics)|

## Stack

- **Oracle 23ai Free** — vector store (`VECTOR(384, FLOAT32)`)
- **FastAPI + uvicorn** — REST + SSE API
- **sentence-transformers** — `all-MiniLM-L6-v2` embeddings
- **LangChain + OpenRouter** — LLM orchestration and streaming
- **RAGAS** — retrieval/answer evaluation
- **React** — frontend

## API

| Method | Path           | Description                                             |
|--------|----------------|---------------------------------------------------------|
| POST   | `/ingest`      | Trigger a CloudWatch ingestion run (background)         |
| POST   | `/query`       | Ask the agent a question; streams tokens via SSE        |
| GET    | `/query/{id}`  | Retrieve a completed query with its RAGAS scores        |
| GET    | `/health`      | Liveness probe                                          |
| GET    | `/metrics`     | Summary stats for the last 24 h of queries              |

## Prerequisites

- Docker (for the Oracle 23ai Free container)
- Conda or Python 3.12
- Node.js 20+
- A `.env` in the repo root with `ORACLE_*`, `AWS_*`, and `OPENROUTER_API_KEY`
  (the file is gitignored; copy your local values in)

## Setup

```bash
conda create -n ragops python=3.12
conda run -n ragops pip install -r requirements.txt
cd frontend && npm install --legacy-peer-deps && cd ..
```

## Run

```bash
./run.sh
```

`run.sh` starts the Oracle container, waits for it to become healthy, applies
the schema, then boots the FastAPI app on `:8000` and the React frontend on
`:3000`.

## Development

Install the pre-commit hooks to run Ruff lint/format and the base hygiene
hooks on every commit:

```bash
pip install pre-commit
pre-commit install
```

The CI workflow (`.github/workflows/ci.yml`) runs these same checks plus an
app-import smoke test and a frontend production build on every push and PR.
