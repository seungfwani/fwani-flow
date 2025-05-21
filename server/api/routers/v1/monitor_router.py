import logging
import math
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper
from core.database import get_db
from core.services.dag_service import get_flow_version
from models.flow_version import FlowVersion
from utils.airflow_client import AirflowClient, get_airflow_client
from utils.functions import split_airflow_dag_id_to_flow_and_version, string2datetime

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/monitor",
    tags=["Monitor"],
)


class EventLogs(BaseModel):
    dag_id: Optional[str]
    version: Optional[int]
    is_draft: Optional[bool]
    run_id: Optional[str]
    event: str
    event_log_id: int
    execution_date: Optional[datetime]
    extra: Optional[str]
    map_index: Optional[int]
    task_id: Optional[str]
    try_number: Optional[int]
    when: Optional[datetime]

    @classmethod
    def from_json(cls, data: dict, flow_version: Optional[FlowVersion] = None):
        return cls(
            dag_id=flow_version.flow_id if flow_version else None,
            version=flow_version.version if flow_version else None,
            is_draft=flow_version.is_draft if flow_version else None,
            run_id=data["run_id"],
            event=data["event"],
            event_log_id=data["event_log_id"],
            execution_date=string2datetime(data["execution_date"]),
            extra=data.get("extra", None),
            map_index=data.get("map_index", None),
            task_id=data.get("task_id", None),
            try_number=data.get("try_number", None),
            when=data.get("when", None),
        )


def parse_comma_separated_ids(
        selected_dag_ids: Optional[str] = Query(None, description="comma-separated dag_ids")
) -> Optional[List[str]]:
    if selected_dag_ids is None:
        return []
    return [v.strip() for v in selected_dag_ids.split(",") if v.strip()]


@router.get("/event-logs")
@api_response_wrapper
async def get_event_log(
        limit: Optional[int] = Query(10, ge=1, le=100, description="페이지 내 limit"),
        page: Optional[int] = Query(1, ge=1, description="페이지 번호"),
        selected_dag_ids: Optional[List[str]] = Depends(parse_comma_separated_ids),
        event_type: Optional[str] = Query(None, description="event 타입"),
        airflow_client: AirflowClient = Depends(get_airflow_client),
        db: Session = Depends(get_db)
):
    """
    모든 airflow 의 이벤트 로그 리스트
    """
    offset = (page - 1) * limit
    uri = f"eventLogs?limit={limit}&offset={offset}&order_by=-when"
    if event_type:
        uri += f"&event={event_type}"
    result = airflow_client.get(uri)
    logs = []
    for row in result.get("event_logs", []):
        if dag_id := row.get("dag_id"):
            flow_id, version, is_draft = split_airflow_dag_id_to_flow_and_version(dag_id)
            if len(selected_dag_ids) > 0 and flow_id not in selected_dag_ids:  # selected_dag_ids 에 해당 X
                continue
            flow_version = get_flow_version(db, flow_id, version, is_draft)
            logs.append(EventLogs.from_json(row, flow_version))
        else:
            if len(selected_dag_ids) > 0:  # dag 관련 X, len(selected_dag_ids) > 0
                continue
            logs.append(EventLogs.from_json(row))

    logger.info(logs)
    total_entries = result.get("total_entries", 0)

    return {
        "event_logs": logs,
        "total_entries": total_entries,
        "total_pages": math.ceil(total_entries / limit),
        "current_entries": len(logs),
        "current_page": page,
    }
