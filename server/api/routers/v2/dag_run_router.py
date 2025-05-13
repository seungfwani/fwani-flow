import json
import logging
import os
import pickle

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import AirflowDagRunModel, TaskInstanceResponse, DAGNode
from config import Config
from core.database import get_db
from core.services.dag_run_service import kill_flow_run, get_flow_run_history, get_all_tasks_by_run_id, \
    get_task_in_run_id, get_task_result_each_tasks
from utils.airflow_client import AirflowClient, get_airflow_client

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag-run",
    tags=["Dag2"],
)


@router.patch("/{run_id}/kill")
@api_response_wrapper
async def kill_dag_run(run_id: str,
                       airflow_client: AirflowClient = Depends(get_airflow_client),
                       db: Session = Depends(get_db)
                       ):
    """
    DAG run 중지
    """
    return kill_flow_run(run_id, airflow_client, db)


@router.get("/{run_id}",
            response_model=APIResponse[AirflowDagRunModel], )
@api_response_wrapper
async def get_dag_run_info(run_id: str,
                           db: Session = Depends(get_db)):
    """
    특정 DAG run 정보 조회
    """
    flow_run = get_flow_run_history(run_id, db)
    if not flow_run:
        raise HTTPException(status_code=404, detail=f"DAG Run [{run_id}] not found")
    return AirflowDagRunModel.from_orm(flow_run)


@router.get("/{run_id}/tasks",
            response_model=APIResponse[TaskInstanceResponse], )
@api_response_wrapper
async def get_tasks_of_dag_run(run_id: str,
                               airflow_client: AirflowClient = Depends(get_airflow_client),
                               db: Session = Depends(get_db)):
    """
    DAG run 의 모든 태스크 리스트 조회
    """
    airflow_dag_run_history, tasks = get_all_tasks_by_run_id(run_id, airflow_client, db)
    return TaskInstanceResponse.from_data(airflow_dag_run_history, tasks)


@router.get("/{run_id}/tasks/{task_id}",
            response_model=APIResponse[DAGNode], )
@api_response_wrapper
async def get_task_of_dag_run(run_id: str, task_id: str,
                              airflow_client: AirflowClient = Depends(get_airflow_client),
                              db: Session = Depends(get_db)):
    """
    DAG run 의 특정 태스크 리스트 조회
    """
    task, ti = get_task_in_run_id(run_id, task_id, airflow_client, db)
    return DAGNode.from_data_with_ti_of_airflow(task, ti)


@router.get("/{run_id}/result")
@api_response_wrapper
async def get_result_of_dag_run(run_id: str, db: Session = Depends(get_db)):
    """
    DAG run 의 결과 데이터 조회
    """
    flow_run = get_flow_run_history(run_id, db)
    return get_dag_result(flow_run.dag_id, flow_run.run_id)


def get_dag_result(dag_id, run_id):
    shared_dir = os.path.abspath(Config.SHARED_DIR)
    result_dir = os.path.join(shared_dir, f"dag_id={dag_id}/run_id={run_id}")
    json_path = os.path.join(result_dir, "final_result.json")
    pkl_path = os.path.join(result_dir, "final_result.pkl")
    if os.path.exists(json_path):
        logger.info(f"load json file: {json_path}")
        with open(json_path, "r") as f:
            return json.load(f)
    elif os.path.exists(pkl_path):
        try:
            logger.info(f"load pickle file: {pkl_path}")
            with open(pkl_path, "rb") as f:
                result = pickle.load(f)
            return {"result": str(result), "type": "pickle"}
        except Exception as e:
            logger.error("⚠️ Failed to load pickle result", e)
            raise
    else:
        raise HTTPException(status_code=404, detail="결과 파일이 존재하지 않습니다.")


@router.get("/{run_id}/tasks/{task_id}/result")
@api_response_wrapper
async def get_result_of_dag_run(run_id: str, task_id: str, db: Session = Depends(get_db)):
    """
    DAG run 의 결과 데이터 조회
    """
    return get_task_result_each_tasks(run_id, task_id, db)
