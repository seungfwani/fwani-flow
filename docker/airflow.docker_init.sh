#!/usr/bin/env bash
wait_for_db

echo "💡 Airflow DB 업그레이드(마이그레이션)…"
airflow db upgrade

echo "👤 Admin 사용자 보장…"
set +e
airflow users list | awk 'NR>2 {print $2}' | grep -Fx "${ROOT_USERNAME}" >/dev/null
USER_EXISTS=$?
set -e
if [[ $USER_EXISTS -ne 0 ]]; then
  airflow users create \
    --username "${ROOT_USERNAME}" \
    --password "${ROOT_PASSWORD}" \
    --firstname "${ROOT_USERNAME}" \
    --lastname "${ROOT_LAST_NAME:-admin}" \
    --role Admin \
    --email "${ROOT_EMAIL}"
else
  echo "✅ ${ROOT_USERNAME} already exists."
fi

echo "✅ Init job finished."