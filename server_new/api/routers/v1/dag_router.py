import logging
from typing import List

from fastapi import APIRouter, Depends
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


@router.patch("/{dag_id}",
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
async def get_dag_list(db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    모든 이용가능한 DAG 리스트를 조회
    """
    dag_service = DagService(db, airflow)
    return dag_service.get_dag_list()
