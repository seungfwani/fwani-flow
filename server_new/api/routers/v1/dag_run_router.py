import logging
from typing import Optional

from fastapi import APIRouter, Depends, Body, Query
from sqlalchemy.orm import Session

from core.airflow_client import AirflowClient, get_airflow_client
from core.database import get_db, get_airflow
from core.services.flow_definition_service import FlowDefinitionService
from core.services.flow_execution_service import FlowExecutionService
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.dag_model import DAGRequest, TaskExecutionModel, MultipleRunRequest

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag-run",
    tags=["Execution"],
)


@router.post("/dag/{dag_id}/run-now",
             response_model=APIResponse[str],
             )
@api_response_wrapper
async def run_dag_immediately(dag_id: str,
                              dag: Optional[DAGRequest] = Body(default=None),
                              db: Session = Depends(get_db),
                              airflow: Session = Depends(get_airflow),
                              airflow_client: AirflowClient = Depends(get_airflow_client)
                              ):
    """
    DAG 실행을 등록하는 API

    dag 를 함께 주면, 해당 상태를 저장하고, 트리거를 요청
    """
    if not dag:
        dag_service = FlowDefinitionService(db, airflow)
        dag_service.update_dag(dag_id, dag)
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.run_execution(dag_id)


@router.post("/dag",
             response_model=APIResponse[bool],
             )
@api_response_wrapper
async def run_dags(dag_ids: MultipleRunRequest,
                   db: Session = Depends(get_db),
                   airflow: Session = Depends(get_airflow),
                   airflow_client: AirflowClient = Depends(get_airflow_client)
                   ):
    """
    여러 dag 를 실행 요청 하는 API
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.register_executions(dag_ids.ids)


@router.delete("/execution/{execution_id}/kill",
               response_model=APIResponse[bool],
               )
@api_response_wrapper
async def kill_dag_run(execution_id: str,
                       db: Session = Depends(get_db),
                       airflow: Session = Depends(get_airflow),
                       airflow_client: AirflowClient = Depends(get_airflow_client)
                       ):
    """
    DAG run 중지
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.kill_execution(execution_id)


@router.get("/execution")
@api_response_wrapper
async def get_dag_run_list(
        db: Session = Depends(get_db),
        airflow: Session = Depends(get_airflow),
        airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """
    DAG run 중지
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.get_execution_list()


@router.get("/execution/{execution_id}/status",
            response_model=APIResponse[str],
            )
@api_response_wrapper
async def get_dag_run_status(execution_id: str,
                             db: Session = Depends(get_db),
                             airflow: Session = Depends(get_airflow),
                             airflow_client: AirflowClient = Depends(get_airflow_client)
                             ):
    """
    DAG 실행의 상태 조회
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.get_execution_status(execution_id)


@router.get("/execution/{execution_id}/tasks",
            response_model=APIResponse[list[TaskExecutionModel]],
            )
@api_response_wrapper
async def get_all_task_instances(execution_id: str,
                                 db: Session = Depends(get_db),
                                 airflow: Session = Depends(get_airflow),
                                 airflow_client: AirflowClient = Depends(get_airflow_client)
                                 ):
    """
    DAG 실행의 모든 태스크 상태 조회
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return [TaskExecutionModel.from_data(t) for t in flow_execution_service.get_all_task_instance(execution_id)]


@router.get("/execution/{execution_id}/tasks/{task_id}/logs",
            response_model=APIResponse[dict],
            )
@api_response_wrapper
async def get_task_logs(execution_id: str,
                        task_id: str,
                        try_number: int = Query(None, description="확인하고 싶은 시도에 대한 log"),
                        db: Session = Depends(get_db),
                        airflow: Session = Depends(get_airflow),
                        airflow_client: AirflowClient = Depends(get_airflow_client)
                        ):
    """
    DAG 실행의 모든 태스크 상태 조회
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.get_task_log(execution_id, task_id, try_number)


@router.get("/execution/{execution_id}/tasks/{task_id}/result",
            response_model=APIResponse[dict],
            )
@api_response_wrapper
async def get_task_result(execution_id: str,
                          task_id: str,
                          db: Session = Depends(get_db),
                          airflow: Session = Depends(get_airflow),
                          airflow_client: AirflowClient = Depends(get_airflow_client)
                          ):
    """
    DAG 실행의 모든 태스크 상태 조회
    """
    flow_execution_service = FlowExecutionService(db, airflow, airflow_client)
    return flow_execution_service.get_task_result_data(execution_id, task_id)
