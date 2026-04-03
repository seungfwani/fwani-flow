#!/bin/bash

export PYTHONPATH=/app/builtin_functions:$PYTHONPATH
wait_for_db
echo "🗓️ DB migrate (idempotent) & start scheduler…"
airflow db upgrade || true
exec airflow scheduler