import json
import logging
import os.path
import pickle
from typing import List

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, DAGResponse
from config import Config
from core.database import get_db
from core.services.dag_service import create_update_draft_dag, publish_flow_version, delete_flow_version, delete_flow, \
    get_flow_last_version_or_draft, get_flows
from models.flow_version import FlowVersion
from utils.airflow_client import get_airflow_client, AirflowClient
from utils.functions import get_airflow_dag_id

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag"],
)


@router.post("",
             response_model=APIResponse[DAGResponse],
             )
@api_response_wrapper
async def draft_dag(dag: DAGRequest, db: Session = Depends(get_db)):
    """DAG draft 버전 생성 및 수정"""
    logger.info(f"Request Data: {dag}")
    return DAGResponse.from_dag(create_update_draft_dag(dag, db))


@router.patch("/{dag_id}",
              response_model=APIResponse[DAGResponse],
              )
@api_response_wrapper
async def publish_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    try:
        return DAGResponse.from_dag(publish_flow_version(dag_id, dag, db))
    except Exception as e:
        db.rollback()
        logger.error(f"DAG 업데이트 실패: {e}")
        raise


@router.delete("/{dag_id}",
               response_model=APIResponse[DAGResponse],
               )
@api_response_wrapper
async def delete_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Delete all DAG versions
    :param dag_id:
    :param db:
    :return:
    """
    return DAGResponse.from_dag(delete_flow(dag_id, db))


@router.delete("/{dag_id}/version/{version}",
               response_model=APIResponse[DAGResponse],
               )
@api_response_wrapper
async def delete_dag_version_api(dag_id: str, version: int, db: Session = Depends(get_db)):
    """
    Delete specific DAG version
    :param dag_id:
    :param db:
    :return:
    """
    return DAGResponse.from_dag(delete_flow_version(dag_id, db, version))


@router.delete("/{dag_id}/draft",
               response_model=APIResponse[DAGResponse],
               )
@api_response_wrapper
async def delete_dag_draft_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Delete draft DAG version
    :param dag_id:
    :param db:
    :return:
    """
    return DAGResponse.from_dag(delete_flow_version(dag_id, db, 0, True))


@router.get("",
            response_model=APIResponse[List[DAGResponse]],
            )
@api_response_wrapper
async def get_dag_list(db: Session = Depends(get_db)):
    """
    Get all available DAG
    :return:
    """
    logger.info(f"▶️ DAG 리스트 조회")
    return [DAGResponse.from_dag(fv) for fv in get_flows(db)]


@router.get("/{dag_id}",
            response_model=APIResponse[DAGResponse],
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Get DAG list
    :return:
    """
    logger.info(f"Get DAG {dag_id}")
    return DAGResponse.from_dag(get_flow_last_version_or_draft(dag_id, db))


@router.post("/{dag_id}/trigger")
@api_response_wrapper
async def get_dag_runs(dag_id: str,
                       airflow_client: AirflowClient = Depends(get_airflow_client),
                       db: Session = Depends(get_db)):
    """
    Run DAG
    :param db:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    response = airflow_client.post(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns", json_data=json.dumps({}))
    logger.info(response)
    return response


@router.patch("/{dag_id}/kill/{dag_run_id}")
@api_response_wrapper
async def kill_dag_run(dag_id: str, dag_run_id: str,
                       airflow_client: AirflowClient = Depends(get_airflow_client),
                       db: Session = Depends(get_db)):
    """
    kill job in DAG
    :param dag_id:
    :param dag_run_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    response = airflow_client.patch(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns/{dag_run_id}", json_data=json.dumps({
        "state": "failed",
    }))
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}")
@api_response_wrapper
async def get_dag_run(dag_id: str, dag_run_id: str,
                      airflow_client: AirflowClient = Depends(get_airflow_client),
                      db: Session = Depends(get_db)):
    """
    get job in DAG
    :param dag_id:
    :param dag_run_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    response = airflow_client.get(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns/{dag_run_id}")
    logger.info(response)
    return response


@router.get("/{dag_id}/history")
@api_response_wrapper
async def get_history_of_dag(dag_id: str,
                             airflow_client: AirflowClient = Depends(get_airflow_client),
                             db: Session = Depends(get_db)):
    """
    get job history of DAG
    :param dag_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    response = airflow_client.get(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns")
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}/tasks")
@api_response_wrapper
async def get_tasks_of_dag_run(dag_id: str, dag_run_id: str,
                               airflow_client: AirflowClient = Depends(get_airflow_client),
                               db: Session = Depends(get_db)):
    """
    get all tasks of DAG_runs
    :param dag_run_id:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    response = airflow_client.get(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns/{dag_run_id}/taskInstances")
    logger.info(f"airflow_client taskInstance response: {response}")
    task_mapper = {t.variable_id: t.id for t in flow_version.tasks}
    logger.info(f"task information for the current DAG: {task_mapper}")
    result = []
    for ti in response.get("task_instances", []):
        task_variable_id = ti['task_id']
        if task_variable_id in task_mapper:
            ti['task_id'] = task_mapper[task_variable_id]
            result.append(ti)
    return result


@router.get("/{dag_id}/dagRuns/{dag_run_id}/tasks/{task_id}")
@api_response_wrapper
async def get_task_of_dag_run(dag_id: str, dag_run_id: str, task_id: str,
                              airflow_client: AirflowClient = Depends(get_airflow_client),
                              db: Session = Depends(get_db)):
    """
    get job history of DAG
    :param flow:
    :param task_id:
    :param dag_run_id:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    task_variable_id = None
    for task in flow_version.tasks:
        if task.id == task_id:
            task_variable_id = task.variable_id
            break
    if task_variable_id is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    response = airflow_client.get(f"dags/{get_airflow_dag_id(flow_version)}/dagRuns/{dag_run_id}/taskInstances/{task_variable_id}")
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}/result")
@api_response_wrapper
async def get_result_of_dag_run(dag_id: str, dag_run_id: str,
                                db: Session = Depends(get_db)):
    """
    get job history of DAG
    :param dag_run_id:
    :param dag_id:
    :return:
    """
    flow_version = get_flow_last_version_or_draft(dag_id, db)
    return get_dag_result(dag_id, dag_run_id, flow_version)


def get_dag_result(dag_id, run_id, flow_version: FlowVersion):
    shared_dir = os.path.abspath(Config.SHARED_DIR)
    result_dir = os.path.join(shared_dir, f"dag_id={get_airflow_dag_id(flow_version)}/run_id={run_id}")
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
