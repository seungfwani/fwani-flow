#!/bin/bash

echo "💡 alembic 데이터베이스 설정..."
alembic upgrade head

echo "💡 built-in function 복사..."
cp /app/builtin_scripts/*.py /app/builtin_functions/

echo "🚀 Workflow 서버 실행..."
python /app/main.py
