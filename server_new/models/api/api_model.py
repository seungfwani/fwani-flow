import logging
import traceback
from functools import wraps
from typing import Optional, TypeVar, Generic

from fastapi import HTTPException
from pydantic import BaseModel, Field

from errors import WorkflowError

logger = logging.getLogger()
T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    success: bool = Field(True, description="Success flag", examples=[True, False])
    message: str = Field(None, description="Error message", examples=["Error: ..."])
    data: Optional[T] = Field(None, description="response data")
    error: Optional[dict] = Field(default_factory=dict, description="error data")


def api_response_wrapper(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
            return APIResponse(
                success=True,
                message="요청이 정상 처리 되었습니다.",
                data=result,
            )
        except WorkflowError as e:
            logger.error(traceback.format_exc())
            return APIResponse(
                success=False,
                message=e.message,
                error={
                    "code": e.code,
                    "detail": ""
                }
            )
        except Exception as e:
            logger.error(traceback.format_exc())
            # handle Unexpected Error
            return APIResponse(
                success=False,
                message="서버 내부 오류가 발생했습니다.",
                error={
                    "code": "INTERNAL_SERVER_ERROR",
                    "detail": str(e),
                    "trace": traceback.format_exc()
                }
            )

    return wrapper
