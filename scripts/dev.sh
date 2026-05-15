#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
# shellcheck source=scripts/dev.env
source "$SCRIPT_DIR/dev.env"

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  deps    Postgres + Airflow만 docker compose로 기동
  server  main.py 실행 (deps가 떠 있어야 함)
  all     deps 기동 → Airflow 준비 대기 → server 실행
  down    compose 서비스 중지
  logs    compose 로그 (postgres, airflow)

Examples:
  ./scripts/dev.sh all
  ./scripts/dev.sh deps
  ./scripts/dev.sh server
EOF
}

cmd="${1:-all}"

case "$cmd" in
  deps)
    exec "$SCRIPT_DIR/dev-deps.sh"
    ;;
  server)
    exec "$SCRIPT_DIR/dev-server.sh"
    ;;
  all)
    "$SCRIPT_DIR/dev-deps.sh"
    wait_for_airflow
    exec "$SCRIPT_DIR/dev-server.sh"
    ;;
  down)
    compose down
    ;;
  logs)
    compose logs -f postgres airflow
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $cmd"
    usage
    exit 1
    ;;
esac
