import logging

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
    dag_service = DagService(dag, db, airflow)
    return dag_service.save_dag()
