import logging
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.database import get_db, get_airflow
from core.services.dag_service import DagService
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.dag_model import DAGResponse, DAGRequest

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
async def save_dag(dag: DAGRequest, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 저장 api
    """
    dag_service = DagService(db, airflow)
    return dag_service.save_dag(dag)


@router.patch("/{dag_id}/update",
              response_model=APIResponse[DAGResponse],
              )
@api_response_wrapper
async def update_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db),
                     airflow: Session = Depends(get_airflow)):
    """
    DAG 저장 api
    """
    dag_service = DagService(db, airflow)
    return dag_service.update_dag(dag_id, dag)


@router.get("",
            response_model=APIResponse[List[DAGResponse]],
            )
@api_response_wrapper
async def get_dag_list(
        is_deleted: bool = Query(False, description="삭제된 DAG 포함 여부"),
        db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    모든 이용가능한 DAG 리스트를 조회
    """
    dag_service = DagService(db, airflow)
    return dag_service.get_dag_list(is_deleted)


@router.get("/{dag_id}",
            response_model=APIResponse[DAGResponse],
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 의 상세 정보 조회
    """
    dag_service = DagService(db, airflow)
    return dag_service.get_dag(dag_id)


@router.delete("/{dag_id}/permanently",
               response_model=APIResponse[str],
               )
@api_response_wrapper
async def delete_dag_permanently(dag_id: str, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 영구 삭제

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = DagService(db, airflow)
    return dag_service.delete_dag_permanently(dag_id)


@router.delete("/{dag_id}/temporary",
               response_model=APIResponse[str],
               )
@api_response_wrapper
async def delete_dag_temporary(dag_id: str, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = DagService(db, airflow)
    return dag_service.delete_dag_temporary(dag_id)


@router.patch("/{dag_id}/restore",
              response_model=APIResponse[str],
              )
@api_response_wrapper
async def restore_dag(dag_id: str, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = DagService(db, airflow)
    return dag_service.restore_deleted_dag(dag_id)
