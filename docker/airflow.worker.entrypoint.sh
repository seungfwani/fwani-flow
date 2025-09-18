#!/bin/bash

export PYTHONPATH=/app/builtin_functions:$PYTHONPATH
wait_for_db
echo "👷 Starting celery worker…"
: "${AIRFLOW__CELERY__BROKER_URL:?Need Celery broker URL}"
exec airflow celery worker