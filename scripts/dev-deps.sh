#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
# shellcheck source=scripts/dev.env
source "$SCRIPT_DIR/dev.env"

ensure_data_dirs

echo "🐳 Postgres + Airflow 기동..."
compose up -d postgres airflow

wait_for_postgres
ensure_workflow_schema

echo ""
echo "📌 서비스"
echo "   Postgres : localhost:65432"
echo "   Airflow UI: http://localhost:8082  (${AIRFLOW_USER}/${AIRFLOW_PASSWORD})"
echo ""
echo "서버 실행: ./scripts/dev-server.sh"
