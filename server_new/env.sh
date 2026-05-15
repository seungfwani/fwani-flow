#!/bin/bash

BASE_DIR=$(pwd)
echo "🚀 Base directory: $BASE_DIR"

# uv 가상환경 (uv sync 후 생성)
if [ -d "$BASE_DIR/.venv" ]; then
  export PATH="$BASE_DIR/.venv/bin:$PATH"
  echo "🚀 uv venv 활성화: $BASE_DIR/.venv"
fi

# ✅ Python Path 설정
export PYTHONPATH="${BASE_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
echo "🚀 PYTHONPATH 설정됨: $PYTHONPATH"
