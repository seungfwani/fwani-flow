import json
import logging

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocal
from core.services.dag_service import get_flow_version
from models.airflow_dag_run_history import AirflowDagRunHistory
from models.flow_trigger_queue import FlowTriggerQueue
from utils.airflow_client import AirflowClient
from utils.functions import split_airflow_dag_id_to_flow_and_version

logger = logging.getLogger()


def process_trigger_queue(db: Session):
    pending_triggers = (db.query(FlowTriggerQueue)
                        .filter_by(status="waiting")
                        .all())

    airflow_client = AirflowClient(
        host=Config.AIRFLOW_HOST,
        port=Config.AIRFLOW_PORT,
        username=Config.AIRFLOW_USER,
        password=Config.AIRFLOW_PASSWORD,
    )
    for trigger in pending_triggers:
        if trigger.try_count >= 5:
            trigger.status = "failed"
            logger.error(f"❌ Trigger failed for {trigger.dag_id}")
            continue
        try:
            response = airflow_client.get(f"dags/{trigger.dag_id}")
            # last_parsed_time = datetime.datetime.strptime(response.get("last_parsed_time"),"%Y-%m-%dT%H:%M:%S.%f+00:00")
            # logger.info(f"last_parsed_time {last_parsed_time}, flow_version_updated_time: {trigger.flow_version.updated_at}")
            # if last_parsed_time < trigger.flow_version.updated_at:
            #     continue
            if response.get("is_paused") is not None:
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
                trigger.try_count += 1
        except Exception as e:
            logger.error(f"❌ Trigger failed for {trigger.dag_id}: {e}", e)
            trigger.status = "failed"

    db.commit()


def trigger_job():
    db = SessionLocal()
    try:
        process_trigger_queue(db)
    finally:
        db.close()
