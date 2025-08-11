import logging
from typing import List, Callable, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.airflow_client import AirflowClient, get_airflow_client
from core.database import get_db, get_airflow
from core.services.flow_definition_service import FlowDefinitionService
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.dag_model import DAGResponse, DAGRequest, ActiveStatusRequest
from utils.functions import to_bool

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag-management",
    tags=["DagManagement"],
)


@router.post("/dag",
             response_model=APIResponse[dict[str, str]],
             )
@api_response_wrapper
async def save_dag(dag: DAGRequest, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 저장 api
    """
    dag_service = FlowDefinitionService(db, airflow)
    return {"id": dag_service.save_dag(dag)}


@router.patch("/dag/{dag_id}/update",
              response_model=APIResponse[dict[str, str]],
              )
@api_response_wrapper
async def update_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    """
    DAG 업데이트 api
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.update_dag(dag_id, dag)}


@router.patch("/dag/{dag_id}/restore",
              response_model=APIResponse[str],
              )
@api_response_wrapper
async def restore_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    return dag_service.restore_deleted_dag(dag_id)


@router.patch("/dag/{dag_id}/active-status",
              response_model=APIResponse[dict[str, bool]],
              )
@api_response_wrapper
async def update_dag_active_status(dag_id: str,
                                   request: ActiveStatusRequest,
                                   db: Session = Depends(get_db),
                                   airflow_client: AirflowClient = Depends(get_airflow_client)
                                   ):
    """
    DAG active 상태 변경 api
    """
    dag_service = FlowDefinitionService(db, airflow_client=airflow_client)
    return {"active_status": dag_service.update_dag_active_status(dag_id, request.active_status)}


@router.delete("/dag/{dag_id}/permanently",
               response_model=APIResponse[dict[str, str]],
               )
@api_response_wrapper
async def delete_dag_permanently(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 영구 삭제

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.delete_dag_permanently(dag_id)}


@router.delete("/dag/{dag_id}/temporary",
               response_model=APIResponse[dict[str, str]],
               )
@api_response_wrapper
async def delete_dag_temporary(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.delete_dag_temporary(dag_id)}


@router.get("/dag-count",
            response_model=APIResponse[dict[str, int]],
            )
@api_response_wrapper
async def get_total_workflow_count(db: Session = Depends(get_db)):
    dag_service = FlowDefinitionService(db)
    return {"total_count": dag_service.get_dag_total_count()}


def parse_comma_query(default: None, alias: str, description: str, cast: Callable[[str], object]):
    def dep(value: Optional[str] = Query(default, alias=alias, description=description)) -> set[object]:
        if not value:
            return set()
        return {cast(v.strip()) for v in value.split(",") if v.strip()}

    return dep


@router.get("/dag",
            response_model=APIResponse[dict[str, List[DAGResponse] | int]],
            )
@api_response_wrapper
async def get_dag_list(
        active_status: set[bool] = Depends(parse_comma_query(None,
                                                             "active_status",
                                                             "active status filter (ex. true,false)",
                                                             to_bool)),
        execution_status: set[str] = Depends(parse_comma_query(None,
                                                               "execution_status",
                                                               "execution status filter (ex. success,failed)",
                                                               str)),
        name: str = Query(None, description="dag name filter"),
        sort: str = Query(None, description="dag sort filter"),
        offset: int = Query(0, description="dag list offset"),
        limit: int = Query(10, description="dag list limit"),
        include_deleted: bool = Query(False, description="삭제된 DAG 포함 여부"),
        db: Session = Depends(get_db)):
    """
    모든 이용가능한 DAG 리스트를 조회
    """
    dag_service = FlowDefinitionService(db)
    dag_list, filtered_count, total_count = dag_service.get_dag_list(active_status,
                                                                     execution_status,
                                                                     name,
                                                                     sort,
                                                                     offset,
                                                                     limit,
                                                                     include_deleted)
    return {
        "total_count": total_count,
        "filtered_count": filtered_count,
        "list": dag_list,
    }


@router.get("/dag/{dag_id}",
            response_model=APIResponse[dict[str, DAGResponse | None]],
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 의 상세 정보 조회
    """
    dag_service = FlowDefinitionService(db)
    return {"origin": dag_service.get_dag(dag_id), "temp": None}


@router.get("/check-dag-name",
            response_model=APIResponse[dict[str, bool]],
            )
@api_response_wrapper
async def check_dag_name(name: str = Query(..., description="체크 할 dag name"), db: Session = Depends(get_db)):
    """
    DAG 이름 체크
    """
    dag_service = FlowDefinitionService(db)
    return {"available": dag_service.check_available_dag_name(dag_name=name)}
