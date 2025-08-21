import datetime
import logging
from typing import List

from sqlalchemy.orm import Session

from core.airflow_client import get_airflow_client_context
from core.database import SessionLocalBaseDB
from errors import WorkflowError
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.enums import FlowExecutionStatus

logger = logging.getLogger()


def process_trigger_queue(db: Session):
    logger.info("🔄 Checking 'waiting' workflow execution queue...")
    waiting_execution: List[FlowExecutionQueue] = (db.query(FlowExecutionQueue)
                                                   .filter_by(status="waiting")
                                                   .all())

    logger.info(f"waiting executions: {waiting_execution}")
    with get_airflow_client_context() as airflow_client:
        for execution in waiting_execution:
            try:
                if execution.status == FlowExecutionStatus.WAITING.value:
                    if execution.flow.file_hash != execution.file_hash:
                        logger.info("⚠️ Do not run. Cause file hash mismatch.")
                        execution.status = FlowExecutionStatus.ERROR.value
                    else:
                        # TODO: try count 를 추가해서 airflow 요청을 재시도 하는 로직 필요
                        run_id = airflow_client.run_dag(execution.dag_id, execution.data)
                        execution.run_id = run_id
                        execution.status = FlowExecutionStatus.TRIGGERED.value
                        execution.triggered_time = datetime.datetime.now(datetime.timezone.utc)
                        logger.info(f"Run request successful for airflow run_id: {run_id}")
            except Exception as e:
                execution.status = FlowExecutionStatus.ERROR.value
                raise WorkflowError(f"DAG 트리거 실패: {e}")
            finally:
                db.add(execution)

    db.commit()
    logger.info("✅ Finished checking 'waiting' workflow execution queue.")


def trigger_job():
    db = SessionLocalBaseDB()
    try:
        process_trigger_queue(db)
    finally:
        db.close()
