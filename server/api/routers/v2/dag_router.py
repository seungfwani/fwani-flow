import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, AirflowDagRunModel
from api.models.trigger_model import TriggerResponse
from core.database import get_db
from core.services.dag_service import register_trigger_last_version_or_draft, get_all_dag_runs_of_all_versions, \
    register_trigger_specific_version

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag2"],
)


@router.post("/{dag_id}/trigger",
             response_model=APIResponse[TriggerResponse],
             )
@api_response_wrapper
async def request_dag_trigger(dag_id: str, dag: Optional[DAGRequest] = Body(default=None),
                              db: Session = Depends(get_db)):
    """
    Run DAG
    :param db:
    :param dag:
    :param dag_id:
    :return:
    """
    return TriggerResponse.from_flow_trigger_queue(register_trigger_last_version_or_draft(dag_id, dag, db))


@router.post("/{dag_id}/version/{version}/trigger",
             response_model=APIResponse[TriggerResponse],
             )
@api_response_wrapper
async def request_dag_trigger(dag_id: str, version: int,
                              db: Session = Depends(get_db)):
    """
    Run DAG
    :param version:
    :param db:
    :param dag_id:
    :return:
    """
    return TriggerResponse.from_flow_trigger_queue(register_trigger_specific_version(dag_id, version, db))


@router.get("/{dag_id}/dagRuns-history",
            response_model=APIResponse[List[AirflowDagRunModel]], )
@api_response_wrapper
async def get_history_of_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    get job history of DAG
    :param dag_id:
    :param airflow_client:
    :return:
    """
    return [AirflowDagRunModel.from_orm(data) for data in get_all_dag_runs_of_all_versions(dag_id, db)]
