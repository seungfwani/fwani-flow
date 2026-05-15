#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
# shellcheck source=scripts/dev.env
source "$SCRIPT_DIR/dev.env"

ensure_data_dirs

if ! compose exec -T postgres pg_isready -U airflow -d airflow >/dev/null 2>&1; then
  echo "⚠️  Postgres가 실행 중이 아닙니다. 먼저 ./scripts/dev-deps.sh 를 실행하세요."
  exit 1
fi

cd "$SERVER_DIR"
# shellcheck source=server_new/env.sh
source "$SERVER_DIR/env.sh"

echo "🚀 Workflow 서버 실행 (http://0.0.0.0:5050) ..."
exec uv run python main.py
