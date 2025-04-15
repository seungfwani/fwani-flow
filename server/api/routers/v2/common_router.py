import logging
from typing import List

from fastapi import APIRouter

from api.models.api_model import APIResponse, api_response_wrapper
from core.enums.data_type_enum import DataType
from core.enums.operator_enum import OperatorType

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/common",
    tags=["Common"],
)


@router.get("/data-types",
            response_model=APIResponse[List[DataType]], )
@api_response_wrapper
async def get_data_types():
    """
    이용 가능한 데이터의 타입 리스트 조회
    """
    return [dt.value for dt in DataType]


@router.get("/operator-types",
            response_model=APIResponse[List[OperatorType]], )
@api_response_wrapper
async def get_operator_types():
    """
    이용 가능한 Operator 타입 리스트 조회
    """
    return [ot.value for ot in OperatorType]
