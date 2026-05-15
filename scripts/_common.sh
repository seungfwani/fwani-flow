#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker-compose-airflow-standalone.yaml}"
SERVER_DIR="$REPO_ROOT/server_new"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

ensure_data_dirs() {
  mkdir -p "$REPO_ROOT/server/data"/{dags,udfs,shared,logs}
  mkdir -p "$SERVER_DIR/data/logs"
}

wait_for_postgres() {
  echo "⏳ Postgres 준비 대기..."
  local i=0
  until compose exec -T postgres pg_isready -U airflow -d airflow >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
      echo "❌ Postgres가 준비되지 않았습니다."
      exit 1
    fi
    sleep 2
  done
  echo "✅ Postgres 준비 완료"
}

ensure_workflow_schema() {
  compose exec -T postgres psql -U airflow -d workflow -v ON_ERROR_STOP=1 \
    -c "CREATE SCHEMA IF NOT EXISTS workflow;" >/dev/null
  echo "✅ workflow 스키마 확인"
}

wait_for_airflow() {
  echo "⏳ Airflow API 준비 대기 (http://localhost:8082)..."
  local i=0
  until curl -sf -o /dev/null -u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
    "http://localhost:8082/api/v1/health" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge 90 ]; then
      echo "⚠️  Airflow API 응답 없음 — 서버는 계속 실행합니다."
      return 0
    fi
    sleep 3
  done
  echo "✅ Airflow API 준비 완료"
}
