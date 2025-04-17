import datetime
import json
import logging
from typing import List

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocal
from core.services.dag_service import get_flow_version
from models.airflow_dag_run_history import AirflowDagRunHistory
from models.flow_trigger_queue import FlowTriggerQueue
from utils.airflow_client import AirflowClient
from utils.functions import split_airflow_dag_id_to_flow_and_version, get_hash, string2datetime

logger = logging.getLogger()


def process_trigger_queue(db: Session):
    pending_triggers: List[FlowTriggerQueue] = (db.query(FlowTriggerQueue)
                                                .filter_by(status="waiting")
                                                .all())

    airflow_client = AirflowClient(
        host=Config.AIRFLOW_HOST,
        port=Config.AIRFLOW_PORT,
        username=Config.AIRFLOW_USER,
        password=Config.AIRFLOW_PASSWORD,
    )
    for trigger in pending_triggers:
        trigger.try_count += 1
        try:
            logger.info(f"Attempting to process ({trigger.id}/{trigger.dag_id}) trigger queue #{{{trigger.try_count}}}")
            if trigger.try_count >= 5:
                trigger.status = "failed"
                logger.error(f"❌ Trigger failed for {trigger.dag_id}")
                continue
            response = airflow_client.get(f"dags/{trigger.dag_id}")
            if not trigger.flow_version.is_loaded_by_airflow:
                last_parsed_time = string2datetime(response.get("last_parsed_time"))
                logger.info(
                    f"last_parsed_time {last_parsed_time}, flow_version_updated_time: {trigger.flow_version.updated_at}")
                if last_parsed_time < trigger.flow_version.updated_at.replace(tzinfo=datetime.timezone.utc):
                    continue
                # 마지막 파싱된 시간이 flow version 의 수정시간 보다 크다면
                # 파일 해쉬 체크
                file_contents = airflow_client.get_content(f"/dagSources/{response.get("file_token")}")
                file_hash = get_hash(file_contents)
                if file_hash != trigger.file_hash:  # 파일이 airflow 에 로딩이 안된 경우
                    logger.info(f"🔁 DAG not ready yet: {trigger.dag_id}")
                    continue
                trigger.flow_version.is_loaded_by_airflow = True

            if response.get("is_paused") is not None:
                if response["is_paused"]:
                    active_result = airflow_client.patch(f"dags/{trigger.dag_id}",
                                                         json_data=json.dumps({"is_paused": False}))
                    logger.info(f"DAG {trigger.dag_id} is activated. {active_result}")
                run_res = airflow_client.post(f"dags/{trigger.dag_id}/dagRuns",
                                              json_data=json.dumps({
                                                  "conf": {
                                                      "source": "api",
                                                  },
                                              }))
                trigger.status = "done"
                # trigger 성공시 airflow 의 반환 값을 저장
                flow_id, version, is_draft = split_airflow_dag_id_to_flow_and_version(run_res["dag_id"])
                flow_version = get_flow_version(db, flow_id, version, is_draft)
                db.add(AirflowDagRunHistory.from_json(flow_version, run_res))
                logger.info(f"🚀 DAG triggered: {trigger.dag_id}, response: {run_res}")
            else:
                logger.info(f"🔁 DAG not ready yet: {trigger.dag_id}")
        except Exception as e:
            logger.error(f"❌ Trigger failed for {trigger.dag_id}: {e}", e)
            trigger.status = "failed"
        finally:
            db.add(trigger)

    db.commit()


def trigger_job():
    db = SessionLocal()
    try:
        process_trigger_queue(db)
    finally:
        db.close()
