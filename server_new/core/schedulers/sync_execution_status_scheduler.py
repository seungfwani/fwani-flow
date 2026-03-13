import logging
from datetime import datetime, timezone, timedelta

from airflow.models import DagRun as AirflowDagRun
from sqlalchemy import and_

from core.airflow_client import get_airflow_client_context
from core.database import SessionLocalBaseDB, SessionLocalAirflowDB
from models.db.flow import Flow
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.enums import FlowExecutionStatus

logger = logging.getLogger()


def sync_dag_run_job(since_minutes: int = 60):
    # UTC now
    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(minutes=since_minutes)

    meta_db = SessionLocalBaseDB()
    airflow_db = SessionLocalAirflowDB()
    try:
        logger.info("▶️ Start to check scheduled dag runs")
        # 시간 필터: execution_date 또는 queued_at 중 하나 선택
        q = (airflow_db.query(AirflowDagRun)
             .filter(
            AirflowDagRun.run_type == "scheduled",
            AirflowDagRun.execution_date >= window_start,
            AirflowDagRun.execution_date < now_utc,
        )
             .yield_per(500))  # 대량 처리 최적화

        for dag_run in q:
            # BUGFIX: .first() 위치 (and_ 안에 넣지 않기)
            execution = (meta_db.query(FlowExecutionQueue)
                         .filter(
                FlowExecutionQueue.dag_id == dag_run.dag_id,
                FlowExecutionQueue.run_id == dag_run.run_id,
            )
                         .first())

            if not execution:
                flow = (meta_db.query(Flow)
                        .filter(Flow.dag_id == dag_run.dag_id)
                        .first())
                if flow is None:
                    continue
                logger.info(f"🔄️ add new execution for {dag_run.dag_id}, {dag_run.run_id}")
                current_flow_snapshot = next(
                    (snap for snap in flow.flow_snapshots if snap.is_current),
                    None,
                )
                if current_flow_snapshot is None and flow.flow_snapshots:
                    current_flow_snapshot = max(flow.flow_snapshots, key=lambda s: s.version)
                meta_db.add(FlowExecutionQueue(
                    flow=flow,
                    flow_snapshot=current_flow_snapshot,
                    dag_id=dag_run.dag_id,
                    run_id=dag_run.run_id,
                    status=FlowExecutionStatus.from_str(dag_run.state).value,
                    scheduled_time=dag_run.execution_date,
                    triggered_time=dag_run.queued_at,
                ))
            else:
                if FlowExecutionStatus.from_str(execution.status) not in FlowExecutionStatus.get_terminal_states():
                    execution.status = FlowExecutionStatus.from_str(dag_run.state).value
                    execution.triggered_time = dag_run.queued_at

        meta_db.commit()
        logger.info("✅ Finished synchronizing scheduled dag runs.")
    finally:
        meta_db.close()
        airflow_db.close()


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
