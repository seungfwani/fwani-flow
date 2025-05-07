import logging
from typing import Optional, List, Tuple

from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, AirflowDagRunModel, DAGResponse
from api.models.trigger_model import TriggerResponse
from core.database import get_db
from core.services.dag_service import register_trigger_last_version_or_draft, get_all_dag_runs_of_all_versions, \
    register_trigger_specific_version, get_flow_last_version, get_flow_version

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag2"],
)


@router.get("/{dag_id}",
            response_model=APIResponse[Tuple[Optional[DAGResponse], Optional[DAGResponse]]],
            description="특정 DAG 조회. [dag, dag] 형태로 리턴.<br/>첫번째: publish 된 *최신 버전*, 두번째: *draft 버전*"
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 의 상세 정보 조회
    """
    logger.info(f"Get DAG {dag_id}")
    return (
        DAGResponse.from_dag(get_flow_last_version(dag_id, db)),
        DAGResponse.from_dag(get_flow_version(db, dag_id, is_draft=True)),
    )


@router.post("/{dag_id}/trigger",
             response_model=APIResponse[TriggerResponse],
             )
@api_response_wrapper
async def request_dag_trigger(dag_id: str, dag: Optional[DAGRequest] = Body(default=None),
                              db: Session = Depends(get_db)):
    """
    DAG 실행
    """
    return TriggerResponse.from_flow_trigger_queue(register_trigger_last_version_or_draft(dag_id, dag, db))


@router.post("/{dag_id}/version/{version}/trigger",
             response_model=APIResponse[TriggerResponse],
             )
@api_response_wrapper
async def request_dag_trigger(dag_id: str, version: int,
                              db: Session = Depends(get_db)):
    """
    특정 DAG 버전을 실행
    """
    return TriggerResponse.from_flow_trigger_queue(register_trigger_specific_version(dag_id, version, db))


@router.get("/{dag_id}/dagRuns-history",
            response_model=APIResponse[List[AirflowDagRunModel]], )
@api_response_wrapper
async def get_history_of_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG Run 리스트 조회
    """
    return [AirflowDagRunModel.from_orm(data) for data in get_all_dag_runs_of_all_versions(dag_id, db)]
