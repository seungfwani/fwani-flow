# trigger_worker.py

import time
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from core.database import SessionLocalBaseDB, SessionLocalAirflowDB
from models.db.flow_execution_queue import FlowExecutionQueue
from services.airflow_client import AirflowClient  # → 직접 만드세요

logger = logging.getLogger()
POLL_INTERVAL = 5  # 초 단위 (주기적으로 실행할 주기)


def execution_worker_loop():
    airflow_client = AirflowClient()

    while True:
        try:
            with SessionLocalBaseDB() as db:
                process_waiting_triggers(db, airflow_client)
        except Exception as e:
            logger.exception(f"TriggerWorker 처리 실패: {e}")

        time.sleep(POLL_INTERVAL)  # 다음 체크까지 대기


def process_waiting_triggers(db: Session, airflow_client: AirflowClient):
    now = datetime.now(timezone.utc)
    waiting_triggers = db.query(FlowExecutionQueue) \
        .filter(FlowExecutionQueue.status == "waiting") \
        .filter(FlowExecutionQueue.scheduled_time <= now) \
        .all()

    for trigger in waiting_triggers:
        try:
            run_id = airflow_client.trigger_dag(trigger.dag_id)
            trigger.status = "triggered"
            trigger.run_id = run_id
            trigger.triggered_time = datetime.now(timezone.utc)
            trigger.try_count += 1
            logger.info(f"✅ DAG 실행 요청됨: {trigger.dag_id} → run_id: {run_id}")
        except Exception as e:
            trigger.status = "error"
            logger.error(f"❌ DAG 트리거 실패: {trigger.dag_id} - {e}")
        finally:
            db.commit()