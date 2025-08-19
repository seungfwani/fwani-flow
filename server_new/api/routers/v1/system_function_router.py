import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.database import get_db
from errors import WorkflowError
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.system_function_model import SystemFunctionResponse
from models.db.system_function import SystemFunction

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/system-function",
    tags=["System Function"],
)


@router.get("",
            response_model=APIResponse[list[SystemFunctionResponse]],
            )
@api_response_wrapper
async def get_system_functions(db: Session = Depends(get_db)):
    """
    system function 리스트 조회
    """
    data = db.query(SystemFunction).filter(SystemFunction.kind != 'meta').all()
    return [SystemFunctionResponse(
        id=sf.id,
        name=sf.name,
        description=sf.description,
        param_schema=sf.param_schema,
        is_deprecated=sf.is_deprecated,
    ) for sf in data]


@router.get("/{system_function_id}",
            response_model=APIResponse[SystemFunctionResponse],
            )
@api_response_wrapper
async def get_system_function(system_function_id: str, db: Session = Depends(get_db)):
    """
    system function 조회
    """
    sf = db.query(SystemFunction).filter(SystemFunction.id == system_function_id).first()
    if not sf:
        raise WorkflowError(f"No such system function [{system_function_id}")
    return SystemFunctionResponse(
        id=sf.id,
        name=sf.name,
        description=sf.description,
        param_schema=sf.param_schema,
        is_deprecated=sf.is_deprecated,
    )
