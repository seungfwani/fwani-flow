import logging

from fastapi import APIRouter, Depends

from api.models.api_model import api_response_wrapper
from utils.airflow_client import AirflowClient, get_airflow_client

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/monitor",
    tags=["Monitor"],
)


@router.get("/event-logs")
@api_response_wrapper
async def get_task_of_dag_run(airflow_client: AirflowClient = Depends(get_airflow_client)):
    """
    get job history of DAG
    :param airflow_client:
    :return:
    """
    response = airflow_client.get(f"eventLogs")
    logger.info(response)
    return response
