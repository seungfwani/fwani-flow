import traceback
from functools import wraps
from typing import Optional, Any

from fastapi import HTTPException
from pydantic import BaseModel


class APIResponse(BaseModel):
    success: bool = True
    message: str
    data: Optional[Any] = None
    error: Optional[dict] = None


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
        except HTTPException as e:
            return APIResponse(
                success=False,
                message=e.detail,
                error={
                    "code": e.status_code,
                    "detail": e.detail
                }
            )
        except Exception as e:
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
