#!/bin/bash

export PYTHONPATH=/app/builtin_functions:$PYTHONPATH
wait_for_db
echo "🌐 Starting webserver…"
exec airflow webserver