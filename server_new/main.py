import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from alembic import command
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from api.routers import v1_routers
from config import Config
from core.log import LOG_CONFIG, setup_logging
from core.schedulers import start_scheduler
from core.sync_system_function import sync_system_functions_from_yaml

logger = logging.getLogger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    start_scheduler()
    yield


def run_migrations():
    """앱 실행 전 Alembic 마이그레이션 적용"""
    alembic_cfg = AlembicConfig("alembic.ini")
    command.upgrade(alembic_cfg, "head")


# Flask 애플리케이션 생성
def init_app():
    logger.info("▶️ Starting server ...")
    # FastAPI 애플리케이션 생성
    description_text = Path("docs/swagger_description.md").read_text(encoding="utf-8")
    app = FastAPI(
        title="Workflow Management API",
        debug=Config.DEBUG,
        lifespan=lifespan,
        description=description_text
    )
    logger.info("✅ FastAPI 애플리케이션 생성 완료")

    # API 라우트 등록
    for router in v1_routers:
        app.include_router(router, prefix="/api/v1")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
    )

    run_migrations()
    sync_system_functions_from_yaml()

    # udf 폴더 생성
    os.makedirs(Config.UDF_DIR, exist_ok=True, mode=0o777)
    os.makedirs(Config.DAG_DIR, exist_ok=True, mode=0o777)
    return app


def start_server():
    uvicorn.run("main:init_app",
                host="0.0.0.0",
                port=5050,
                reload=True,
                log_config=LOG_CONFIG,
                log_level=Config.LOG_LEVEL.lower(),
                )


if __name__ == "__main__":
    start_server()
