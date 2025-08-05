import datetime
import logging
from enum import Enum

from airflow.models import TaskInstance as AirflowTaskInstance
from models.domain.task_instance import TaskInstance as DomainTaskInstance
from sqlalchemy.orm import Session

from config import Config
from errors import WorkflowError
from models.db.flow import Flow as DBFlow
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.enums import FlowExecutionStatus
from utils.airflow_client import AirflowClient

logger = logging.getLogger()


class FlowExecutionService:
    def __init__(self, meta_db: Session, airflow_db: Session, airflow_client: AirflowClient):
        self.meta_db = meta_db
        self.airflow_db = airflow_db
        self.airflow_client = airflow_client

    def _get_flow(self, flow_id: str) -> DBFlow:
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == flow_id).first()
        if not flow:
            raise WorkflowError(f"등록되지 않은 DAG ({flow_id}")
        return flow

    def _get_flow_execution(self, execution_id: str) -> DBFlow:
        execution = self.meta_db.query(FlowExecutionQueue).filter_by(id=execution_id).first()
        if not execution:
            raise WorkflowError(f"없는 실행 {execution_id}")
        return execution

    def _register_execution(self, flow_id: str) -> FlowExecutionQueue:
        flow = self._get_flow(flow_id)
        flow_execution = FlowExecutionQueue(
            flow_id=flow.id,
            dag_id=flow.dag_id,
            status=FlowExecutionStatus.WAITING.value,
            file_hash=flow.file_hash,
            scheduled_time=datetime.datetime.now(),
            triggered_time=datetime.datetime.now(),
            data={
                "conf": {
                    "source": "api",
                },
            },
        )
        self.meta_db.add(flow_execution)
        self.meta_db.commit()

        return flow_execution

    def _request_airflow_dag_run(self, flow_execution: FlowExecutionQueue):
        try:
            if FlowExecutionStatus(flow_execution.status) == FlowExecutionStatus.WAITING:
                if flow_execution.flow.file_hash != flow_execution.file_hash:
                    flow_execution.status = FlowExecutionStatus.ERROR.value
                else:
                    # TODO: try count 를 추가해서 airflow 요청을 재시도 하는 로직 필요
                    run_id = self.airflow_client.run_dag(flow_execution.dag_id, flow_execution.data)
                    flow_execution.run_id = run_id
                    flow_execution.status = FlowExecutionStatus.TRIGGERED.value
                    flow_execution.triggered_time = datetime.datetime.now(datetime.timezone.utc)
        except Exception as e:
            flow_execution.status = FlowExecutionStatus.ERROR.value
            raise WorkflowError(f"DAG 트리거 실패: {e}")
        finally:
            self.meta_db.commit()

    def run_execution(self, flow_id: str):
        flow_execution = self._register_execution(flow_id)
        # 즉시 실행일 경우 → 바로 실행
        self._request_airflow_dag_run(flow_execution)
        return flow_execution.id

    def kill_execution(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)

        status = self.airflow_client.kill(execution.dag_id, execution.run_id)
        execution.status = FlowExecutionStatus.from_str(status)
        self.meta_db.commit()

    def cancel_execution(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)
        if execution.status != FlowExecutionStatus.WAITING.value:
            raise WorkflowError("취소할 수 없는 트리거")

        execution.status = FlowExecutionStatus.CANCELED.value
        self.meta_db.commit()

    def get_execution_status(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)

        if FlowExecutionStatus(execution.status) not in FlowExecutionStatus.get_terminal_states():
            # 아직 대기중/진행중 이므로, 상태 체크 후 반환
            new_status = self.airflow_client.get_status(execution.dag_id, execution.run_id)
            execution.status = FlowExecutionStatus.from_str(new_status).value
            self.meta_db.commit()
        return execution.status

    def get_all_task_instance(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)
        task_instances = (self.airflow_db.query(AirflowTaskInstance)
                          .filter(
            (AirflowTaskInstance.dag_id == execution.dag_id) and (AirflowTaskInstance.run_id == execution.run_id))
                          .all())
        task_dict = {task.variable_id: task for task in execution.flow.tasks}

        return [DomainTaskInstance(
            task_id=task_dict[ti.task_id].id,
            execution_date=ti.execution_date,
            start_date=ti.start_date,
            end_date=ti.end_date,
            duration=ti.duration,
            operator=ti.operator,
            queued_when=ti.queued_dttm,
            status=FlowExecutionStatus.from_str(ti.state),
            try_number=ti.try_number,
        ) for ti in task_instances]


#     def kill_dag_run(self, dag_id: str, run_id: str):
#         # Airflow REST API 또는 DB 직접 update
#         response = requests.delete(f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}")
#         if response.status_code != 204:
#             raise WorkflowError("DAG Run 종료 실패")
#
#     def get_trigger_status(self, trigger_id: str) -> str:
#         trigger = self.meta_db.query(FlowExecutionQueue).filter_by(id=trigger_id).first()
#         if not trigger:
#             raise WorkflowError("트리거 없음")
#
#         if trigger.airflow_run_id:
#             # airflow DB에서 상태 확인
#             run = self.airflow_db.query(DagRun).filter_by(run_id=trigger.airflow_run_id).first()
#             return run.state if run else trigger.status
#         else:
#             return trigger.status
#
#     def get_trigger_log(self, flow_id: str):
#         ...
#
#
# def get_flow_run_history(run_id: str, db: Session) -> AirflowDagRunHistory:
#     return db.query(AirflowDagRunHistory).filter(
#         AirflowDagRunHistory.id == run_id,
#     ).first()
#
#
# def kill_flow_run(run_id: str, airflow_client: AirflowClient, db: Session):
#     flow_run = get_flow_run_history(run_id, db)
#     response = airflow_client.patch(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}",
#                                     json_data=json.dumps({
#                                         "state": "failed",
#                                     }))
#     logger.info(response)
#     return response
#
#
# def get_all_tasks_by_run_id(run_id: str, airflow_client: AirflowClient, db: Session) \
#         -> Tuple[AirflowDagRunHistory, List[Tuple[AirflowDagRunSnapshotTask, dict]]]:
#     flow_run = get_flow_run_history(run_id, db)
#     response = airflow_client.get(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}/taskInstances")
#     logger.info(f"airflow_client taskInstance response: {response}")
#     task_mapper = {t.variable_id: t for t in flow_run.snapshot_tasks}
#     logger.info(f"task information for the current DAG: {task_mapper}")
#
#     task_instance_data = []
#     for ti in response.get("task_instances", []):
#         task_variable_id = ti['task_id']
#         if task_variable_id in task_mapper:
#             ti['task_id'] = task_mapper[task_variable_id].task_id
#             task_instance_data.append((task_mapper[task_variable_id], ti))
#     return flow_run, task_instance_data
#
#
# def get_task_in_run_id(run_id: str, task_id: str, airflow_client: AirflowClient, db: Session):
#     flow_run = get_flow_run_history(run_id, db)
#     airflow_task_id = None
#     for task in flow_run.flow_version.tasks:
#         if task.id == task_id:
#             airflow_task_id = task.variable_id
#             break
#     if airflow_task_id is None:
#         raise ValueError(f"Task {task_id} not found")
#
#     response = airflow_client.get(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}/taskInstances/{airflow_task_id}")
#     logger.info(f"airflow_client taskInstance response: {response}")
#     task_mapper = {t.variable_id: t for t in flow_run.snapshot_tasks}
#     logger.info(f"task information for the current DAG: {task_mapper}")
#
#     task_variable_id = response['task_id']
#     if task_variable_id in task_mapper:
#         response['task_id'] = task_mapper[task_variable_id].task_id
#         return task_mapper[task_variable_id], response
#     else:
#         raise ValueError(f"Task {task_id} not found")
#
#
# def get_snapshot_task_by_id(task_id: str, db: Session) -> (AirflowDagRunSnapshotTask, FunctionLibrary):
#     task: AirflowDagRunSnapshotTask = (db.query(AirflowDagRunSnapshotTask)
#                                        .filter(AirflowDagRunSnapshotTask.task_id == task_id)
#                                        .first())
#     if task is None:
#         raise HTTPException(status_code=404, detail=f"Task({task_id}) 가 존재하지 않습니다.")
#
#     function_ = db.query(FunctionLibrary).filter(FunctionLibrary.id == task.function_id).first()
#     return task, function_
#
#
# def get_task_result_each_tasks(run_id: str, task_id: str, db: Session):
#     flow_run = get_flow_run_history(run_id, db)
#     task, function_ = get_snapshot_task_by_id(task_id, db)
#
#     airflow_dag_id = flow_run.dag_id
#     airflow_run_id = flow_run.run_id
#
#     shared_dir = os.path.abspath(Config.SHARED_DIR)
#     result_dir = os.path.join(shared_dir, f"dag_id={airflow_dag_id}/run_id={airflow_run_id}")
#     pkl_path = os.path.join(result_dir, f"{task.variable_id}.pkl")
#     if os.path.exists(pkl_path):
#         try:
#             logger.info(f"load pickle file: {pkl_path}")
#             with open(pkl_path, "rb") as f:
#                 result = pickle.load(f)
#             return {"result": result, "type": function_.output.type}
#         except Exception as e:
#             logger.error("⚠️ Failed to load pickle result", e)
#             raise
#     else:
#         raise HTTPException(status_code=404, detail="결과 파일이 존재하지 않습니다.")
#
#
# def get_task_logs(run_id: str, task_id: str, try_number: int, airflow_client: AirflowClient, db: Session) \
#         -> Tuple[str, str]:
#     flow_run = get_flow_run_history(run_id, db)
#     task, function_ = get_snapshot_task_by_id(task_id, db)
#     airflow_dag_id = flow_run.dag_id
#     airflow_run_id = flow_run.run_id
#     airflow_task_id = task.variable_id
#     task_instance_try = airflow_client.get(
#         f"dags/{airflow_dag_id}/dagRuns/{airflow_run_id}/taskInstances/{airflow_task_id}/tries/{try_number}")
#     status = task_instance_try.get("state")
#     log = airflow_client.get_content(
#         f"dags/{airflow_dag_id}/dagRuns/{airflow_run_id}/taskInstances/{airflow_task_id}/logs/{try_number}")
#     return status, log
if __name__ == "__main__":
    print(FlowExecutionStatus("waiting"))
