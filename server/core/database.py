import importlib
import logging
import os
import pkgutil

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session

from config import Config

logger = logging.getLogger(__name__)


class ReadOnlySession(Session):
    def flush(self, *args, **kwargs):
        raise RuntimeError("This session is read-only!")

    def commit(self):
        raise RuntimeError("This session is read-only!")


def get_engine(uri):
    schema = uri.split("://", 1)[0]
    if schema == "postgresql":
        engine = create_engine(uri)
    elif schema == "sqlite":
        engine = create_engine(uri, connect_args={"check_same_thread": False})
    else:
        raise HTTPException(status_code=404, detail="Invalid database URI")
    return engine


SessionLocalBaseDB = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=get_engine(Config.DB_URI)))
SessionLocalAirflowDB = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=get_engine(Config.AIRFLOW_DB_URI), class_=ReadOnlySession))

BaseDB = declarative_base()
AirflowDB = declarative_base()


# ✅ `models/` 폴더 내 모든 `.py` 파일을 자동으로 import하여 Alembic이 감지할 수 있도록 설정
def import_models():
    models_package = "models"  # models 폴더 지정
    models_path = os.path.join(os.path.dirname(__file__), "../", models_package)

    for _, module_name, _ in pkgutil.iter_modules([models_path]):
        importlib.import_module(f"{models_package}.{module_name}")


# 자동 모델 감지 실행
import_models()


def get_db():
    logger.info("Connecting to database")
    db = SessionLocalBaseDB()
    try:
        yield db
    finally:
        db.close()
