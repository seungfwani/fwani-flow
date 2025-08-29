import logging

from airflow.models import DagRun as AirflowDagRun
from sqlalchemy import and_

from core.airflow_client import get_airflow_client, get_airflow_client_context
from core.database import SessionLocalBaseDB, SessionLocalAirflowDB
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.enums import FlowExecutionStatus

logger = logging.getLogger()


def sync_execution_status_job():
    meta_db = SessionLocalBaseDB()
    airflow_db = SessionLocalAirflowDB()
    try:
        logger.info("▶️ Start to check not terminated status")
        terminated_status = list(FlowExecutionStatus.get_terminal_states())
        terminated_status.append(FlowExecutionStatus.WAITING)
        non_terminated_execution: list[FlowExecutionQueue] = (
            meta_db.query(FlowExecutionQueue)
            .filter(FlowExecutionQueue.status.notin_(
                [status.value for status in terminated_status]
            ))
            .all())
        logger.info(f"non terminated executions: {len(non_terminated_execution)}")
        with get_airflow_client_context() as airflow_client:
            for execution in non_terminated_execution:
                if execution.flow_snapshot_id is None:
                    logger.warning(f"⚠️ kill execution({execution.id}) cause snapshot is deleted.")
                    airflow_client.kill(execution.dag_id, execution.run_id)
                    execution.status = FlowExecutionStatus.KILLED.value
                else:
                    # 아직 대기중/진행중 이므로, 상태 체크 후 반환
                    logger.info(f"🔄️ check status execution({execution.id})")
                    airflow_dag_run = (airflow_db.query(AirflowDagRun)
                                       .filter(and_(AirflowDagRun.dag_id == execution.dag_id,
                                                    AirflowDagRun.run_id == execution.run_id))
                                       .first())
                    if airflow_dag_run:
                        execution.status = FlowExecutionStatus.from_str(airflow_dag_run.state).value

        meta_db.commit()
        logger.info("✅ Finished synchronizing execution status.")
    finally:
        meta_db.close()
        airflow_db.close()
