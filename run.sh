#!/usr/bin/env bash
# RAG-Ops local dev launcher.
#   Starts Oracle (container), FastAPI (:8000), and the React frontend (:3000).
#   Requires: conda env "ragops", Docker, Node, and a .env file in this directory.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

PY="$HOME/miniforge3/envs/ragops/bin/python"
[ -x "$PY" ] || { echo "missing conda env 'ragops' (create with: conda create -n ragops python=3.12)"; exit 1; }
[ -f .env ] || { echo "missing .env (copy from /mnt/z/shared/.env)"; exit 1; }

set -a; source .env; set +a

# ── Oracle ─────────────────────────────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -qx ragops_oracle; then
  echo "[run] starting Oracle 23ai container…"
  docker run -d --name ragops_oracle \
    -e ORACLE_PWD="$ORACLE_PASSWORD" \
    -e ORACLE_CHARACTERSET=AL32UTF8 \
    -p 1521:1521 -p 5500:5500 \
    -v ragops_oracle_data:/opt/oracle/oradata \
    -v "$PWD/schema:/docker-entrypoint-initdb.d:ro" \
    container-registry.oracle.com/database/free:latest
fi

echo "[run] waiting for Oracle to become healthy (first boot can take a few minutes)…"
for i in $(seq 1 120); do
  if docker exec ragops_oracle bash -lc "echo 'select 1 from dual;' | sqlplus -s '$ORACLE_USER/$ORACLE_PASSWORD@//localhost:1521/$ORACLE_SERVICE' >/dev/null 2>&1"; then
    echo "[run] Oracle ready (after ${i}x5s)"
    break
  fi
  [ "$i" -eq 120 ] && { echo "[run] Oracle not ready in time"; exit 1; }
  sleep 5
done

# Apply the schema idempotently (container init only runs on first boot).
# clean_schema.sql references the ragops user's tables, so ensure that user
# exists and is granted the Oracle vector/table privileges first, then run it.
echo "[run] ensuring schema…"
docker exec ragops_oracle bash -lc "sqlplus -s 'SYS/$ORACLE_PASSWORD@//localhost:1521/FREE as sysdba' <<'SQL'
CREATE USER $ORACLE_USER IDENTIFIED BY \"$ORACLE_PASSWORD\" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CONNECT, RESOURCE, CREATE ANY VECTOR INDEX TO $ORACLE_USER;
EXIT;
SQL" >/dev/null 2>&1 || true

# Idempotent: the DDL is guarded by checks in the schema file, safe to re-run.
docker exec ragops_oracle bash -lc "
  sqlplus -s '$ORACLE_USER/$ORACLE_PASSWORD@//localhost:1521/$ORACLE_SERVICE' <<'SQL'
  @clean_schema.sql
  EXIT;
SQL
" >/dev/null 2>&1 || true

# ── API ────────────────────────────────────────────────────────────────────
echo "[run] starting FastAPI on :8000…"
"$PY" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &

# ── Frontend ───────────────────────────────────────────────────────────────
echo "[run] starting frontend on :3000…"
(cd frontend && npm start)

wait
