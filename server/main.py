import os

import uvicorn
from fastapi import FastAPI

from api.routers import routers
from config import Config


# Flask 애플리케이션 생성
def create_app():
    # FastAPI 애플리케이션 생성
    app = FastAPI(
        title="Workflow Management API",
        debug=Config.DEBUG,
    )

    # API 라우트 등록
    for router in routers:
        app.include_router(router, prefix="/api/v1")

    # udf 폴더 생성
    os.makedirs(Config.UDF_DIR, exist_ok=True)

    return app


def start_server():
    uvicorn.run("server.main:create_app", host="0.0.0.0", port=5050, reload=True)


if __name__ == "__main__":
    start_server()
