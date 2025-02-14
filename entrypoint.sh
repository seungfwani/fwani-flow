#!/bin/bash

echo "💡 Airflow 데이터베이스 초기화..."
airflow db init

echo "🔄 Airflow 사용자 추가..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Seunghwan \
    --lastname Seo \
    --role Admin \
    --email myuser@example.com


echo "🚀 PyCharm Remote Debug 활성화..."
python -m debugpy --listen 0.0.0.0:5678 --wait-for-client -m airflow webserver &  # Debug 모드

echo "🚀 Airflow Standalone 실행..."
airflow standalone
