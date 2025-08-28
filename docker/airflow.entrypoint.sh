#!/bin/bash

echo "💡 Airflow 데이터베이스 초기화..."
airflow db init

echo "🔄 Airflow 사용자 추가..."
airflow users create \
    --username ${ROOT_USERNAME:-fwani} \
    --password ${ROOT_PASSWORD:-fwani} \
    --firstname ${ROOT_USERNAME:-fwani} \
    --lastname ${ROOT_LAST_NAME:-seo} \
    --role Admin \
    --email ${ROOT_EMAIL:-seungfwani2@mobigen.com} || echo "⚠️ Admin 계정이 이미 존재하거나 생성 실패."

#echo "🚀 PyCharm Remote Debug 활성화..."
#python -m debugpy --listen 0.0.0.0:5678 --wait-for-client --log-to-stderr -m airflow webserver &  # Debug 모드
export PYTHONPATH=/app/builtin_functions:$PYTHONPATH

echo "🚀 Airflow Standalone 실행..."
airflow standalone
