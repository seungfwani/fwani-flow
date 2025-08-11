import datetime
import logging
import os
import pickle

from airflow.models import DagRun as AirflowDagRun
from airflow.models import TaskInstance as AirflowTaskInstance
from sqlalchemy import and_
from sqlalchemy.orm import Session

from config import Config
from core.airflow_client import AirflowClient
from errors import WorkflowError
from models.api.dag_model import ExecutionResponse
from models.db.flow import Flow as DBFlow
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.enums import FlowExecutionStatus
from models.domain.task_instance import TaskInstance as DomainTaskInstance

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

    def register_executions(self, dag_ids: list[str]):
        for dag_id in dag_ids:
            _ = self._register_execution(dag_id)
        return True

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

    def get_execution_list(self):
        executions = self.meta_db.query(FlowExecutionQueue).all()
        return [ExecutionResponse(
            id=execution.id,
            flow_id=execution.flow_id,
            status=execution.status,
            scheduled_time=execution.scheduled_time,
            triggered_time=execution.triggered_time,
        ) for execution in executions]

    def get_execution_status(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)

        if FlowExecutionStatus(execution.status) not in FlowExecutionStatus.get_terminal_states():
            # 아직 대기중/진행중 이므로, 상태 체크 후 반환
            airflow_dag_run = (self.airflow_db.query(AirflowDagRun)
                               .filter(and_(AirflowDagRun.dag_id == execution.dag_id,
                                            AirflowDagRun.run_id == execution.run_id))
                               .first())
            if airflow_dag_run:
                execution.status = airflow_dag_run.state
                self.meta_db.commit()
        return execution.status

    def get_all_task_instance(self, execution_id: str):
        execution = self._get_flow_execution(execution_id)
        task_instances = (self.airflow_db.query(AirflowTaskInstance)
                          .filter(and_(AirflowTaskInstance.dag_id == execution.dag_id,
                                       AirflowTaskInstance.run_id == execution.run_id))
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

    def get_task_log(self, execution_id: str, task_id: str, try_number: int = None):
        result = {
            "status": None,
            "log": None,
        }
        execution = self._get_flow_execution(execution_id)
        task = None
        for t in execution.flow.tasks:
            if t.id == task_id:
                task = t
                break
        if task is None:
            return result
        task_instance = (self.airflow_db.query(AirflowTaskInstance)
                         .filter(and_(AirflowTaskInstance.dag_id == execution.dag_id,
                                      AirflowTaskInstance.run_id == execution.run_id,
                                      AirflowTaskInstance.task_id == task.variable_id))
                         .first())
        if task_instance is None:
            return result

        request_try_number = try_number if try_number is not None else task_instance.try_number
        status, log = self.airflow_client.get_task_log(execution.dag_id, execution.run_id, task_instance.task_id,
                                                       request_try_number)
        result["try_number"] = request_try_number
        result["status"] = FlowExecutionStatus.from_str(status)
        result["log"] = log
        return result

    def get_task_result_data(self, execution_id: str, task_id: str):
        execution = self._get_flow_execution(execution_id)
        task = None
        for t in execution.flow.tasks:
            if t.id == task_id:
                task = t
                break

        pkl_path = os.path.join(Config.SHARED_DIR,
                                f"dag_id={execution.dag_id}",
                                f"run_id={execution.run_id}",
                                f"task_id={task.variable_id}",
                                "result.pkl")
        if os.path.exists(pkl_path):
            try:
                logger.info(f"load pickle file: {pkl_path}")
                with open(pkl_path, "rb") as f:
                    result = pickle.load(f)
                return {"result": result, "type": "dict"}
            except Exception as e:
                logger.error("⚠️ Failed to load pickle result", e)
                raise WorkflowError("Failed to load pickle result")
        else:
            raise WorkflowError("결과 파일이 존재하지 않습니다.")


if __name__ == "__main__":
    print(FlowExecutionStatus("waiting"))
