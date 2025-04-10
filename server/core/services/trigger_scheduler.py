import json
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocal
from models.flow_trigger_queue import FlowTriggerQueue
from utils.airflow_client import AirflowClient

logger = logging.getLogger()

scheduler = BackgroundScheduler()


def process_trigger_queue(db: Session):
    pending_triggers = db.query(FlowTriggerQueue).filter_by(status="waiting").all()

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
        trigger.try_count += 1
        try:
            response = airflow_client.get(f"dags/{trigger.dag_id}")
            # last_parsed_time = datetime.datetime.strptime(response.get("last_parsed_time"),"%Y-%m-%dT%H:%M:%S.%f+00:00")
            # logger.info(f"last_parsed_time {last_parsed_time}, flow_version_updated_time: {trigger.flow_version.updated_at}")
            # if last_parsed_time < trigger.flow_version.updated_at:
            #     continue
            if response.get("is_paused") is not None:
                run_res = airflow_client.post(f"dags/{trigger.dag_id}/dagRuns", json_data=json.dumps({}))
                trigger.run_id = run_res.get("dag_run_id")
                trigger.status = run_res.get("state")
                logger.info(f"🚀 DAG triggered: {trigger.dag_id}, response: {run_res}")
            else:
                logger.info(f"🔁 DAG not ready yet: {trigger.dag_id}")
        except Exception as e:
            logger.error(f"❌ Trigger failed for {trigger.dag_id}: {e}")

    db.commit()


def trigger_job():
    db = SessionLocal()
    try:
        process_trigger_queue(db)
    finally:
        db.close()


def start_scheduler():
    scheduler.add_job(trigger_job, "interval", seconds=10)
    scheduler.start()
