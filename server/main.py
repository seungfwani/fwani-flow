import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from alembic import command
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from api.routers import v1_routers, v2_routers
from config import Config
from core.log import LOG_CONFIG, setup_logging
from core.scheduler import start_scheduler

logger = logging.getLogger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    yield


def run_migrations():
    """앱 실행 전 Alembic 마이그레이션 적용"""
    alembic_cfg = AlembicConfig("alembic.ini")
    command.upgrade(alembic_cfg, "head")


# Flask 애플리케이션 생성
def init_app():
    # FastAPI 애플리케이션 생성
    app = FastAPI(
        title="Workflow Management API",
        debug=Config.DEBUG,
        lifespan=lifespan,
    )
    # API 라우트 등록
    for router in v1_routers:
        app.include_router(router, prefix="/api/v1")
    for router in v2_routers:
        app.include_router(router, prefix="/api/v2")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
    )

    run_migrations()

    # udf 폴더 생성
    os.makedirs(Config.UDF_DIR, exist_ok=True, mode=0o777)
    os.makedirs(Config.DAG_DIR, exist_ok=True, mode=0o777)

    # 실행 시 자동 설정 적용
    setup_logging()
    logger.info("✅ FastAPI 애플리케이션 생성 완료")
    return app


def start_server():
    logger.info("Starting server ...")
    uvicorn.run("main:init_app",
                host="0.0.0.0",
                port=5050,
                reload=True,
                log_config=LOG_CONFIG,
                log_level=Config.LOG_LEVEL.lower(),
                )


if __name__ == "__main__":
    start_server()
