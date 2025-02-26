import logging
import os

import uvicorn
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from alembic import command
from api.routers import routers
from config import Config
from core.log import LOG_CONFIG

logger = logging.getLogger()


def run_migrations():
    """앱 실행 전 Alembic 마이그레이션 적용"""
    alembic_cfg = AlembicConfig("alembic.ini")
    command.upgrade(alembic_cfg, "head")


# Flask 애플리케이션 생성
def create_app():
    # FastAPI 애플리케이션 생성
    app = FastAPI(
        title="Workflow Management API",
        debug=Config.DEBUG,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
    )

    # API 라우트 등록
    for router in routers:
        app.include_router(router, prefix="/api/v1")

    run_migrations()

    # udf 폴더 생성
    os.makedirs(Config.UDF_DIR, exist_ok=True)
    os.makedirs(Config.DAG_DIR, exist_ok=True)

    return app


def start_server():
    logger.info("Starting server ...")
    uvicorn.run("server.main:create_app",
                host="0.0.0.0",
                port=5050,
                reload=True,
                log_config=LOG_CONFIG,
                log_level="info",
                )


if __name__ == "__main__":
    start_server()
