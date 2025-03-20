#!/bin/bash

echo "💡 alembic 데이터베이스 설정..."
alembic upgrade head

echo "🚀 Workflow 서버 실행..."
python /app/main.py
