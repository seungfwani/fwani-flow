import logging

from fastapi import APIRouter, Query

from api.render_template import render_udf_code_block
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.code_block_model import CodeRequest
from utils.code_validator import validate_user_code

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/code-block",
    tags=["code-block"],
)


@router.get("",
            response_model=APIResponse[str],
            )
@api_response_wrapper
async def get_code_block(
        params_count: int = Query(2, description="run 함수의 파라미터 개수"),
):
    return render_udf_code_block(params_count)


@router.post("/validate",
             response_model=APIResponse[list[str]],
             )
@api_response_wrapper
async def check_code_validate(
        code: CodeRequest
):
    return validate_user_code(code.code)
