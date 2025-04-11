import datetime

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocal
from core.services.dag_service import get_flow_version
from models.airflow_dag_run_history import AirflowDagRunHistory
from models.flow_version import FlowVersion
from utils.airflow_client import AirflowClient
from utils.functions import string2datetime, get_airflow_dag_id, split_airflow_dag_id_to_flow_and_version


def sync_dag_runs_from_airflow(dag_id: str, db: Session):
    airflow_client = AirflowClient(
        host=Config.AIRFLOW_HOST,
        port=Config.AIRFLOW_PORT,
        username=Config.AIRFLOW_USER,
        password=Config.AIRFLOW_PASSWORD,
    )
    response = airflow_client.get(f"dags/{dag_id}/dagRuns")
    dag_runs = response.get("dag_runs", [])

    for run in dag_runs:
        run_id = run["dag_run_id"]
        existing: AirflowDagRunHistory = db.query(AirflowDagRunHistory).filter_by(run_id=run_id).first()

        if existing:
            is_updated = False
            # 변경된 경우에만 업데이트
            if existing.status != run["state"]:
                existing.status = run["state"]
                is_updated = True
            if run["end_date"] and existing.end_date != string2datetime(run["end_date"]):
                existing.end_date = string2datetime(run["end_date"])
                is_updated = True
            if is_updated:
                existing.updated_at = datetime.datetime.now(datetime.UTC)
        else:
            # 신규 데이터 insert
            flow_id, version, is_draft = split_airflow_dag_id_to_flow_and_version(run["dag_id"])
            flow_version = get_flow_version(db, flow_id, version, is_draft)
            history = AirflowDagRunHistory.from_json(flow_version, run)
            db.add(history)

    db.commit()


def trigger_sync_job():
    db = SessionLocal()
    try:
        all_fv = db.query(FlowVersion).distinct()
        for fv in all_fv:
            dag_id = get_airflow_dag_id(fv)
            sync_dag_runs_from_airflow(dag_id, db)
    finally:
        db.close()
