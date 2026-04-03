import importlib
import logging
import os
import pkgutil
from contextlib import contextmanager

from fastapi import HTTPException
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session

from config import Config

logger = logging.getLogger()


class ReadOnlySession(Session):
    def flush(self, *args, **kwargs):
        raise RuntimeError("This session is read-only!")

    def commit(self):
        raise RuntimeError("This session is read-only!")


def create_postgres_engine(uri: str, schema: str | None = None):
    if schema:
        engine = create_engine(uri,
                               echo_pool=True,  # 풀 이벤트 로그 활성화
                               pool_pre_ping=True,  # 끊어진 커넥션 감지 권장
                               connect_args={
                                   "options": f"-c search_path={Config.AIRFLOW_DB_SCHEMA}"
                               },
                               )
    else:
        engine = create_engine(uri,
                               echo_pool=True,  # 풀 이벤트 로그 활성화
                               pool_pre_ping=True,  # 끊어진 커넥션 감지 권장
                               )
    return engine


def create_sqlite_engine(uri: str):
    engine = create_engine(uri,
                           connect_args={"check_same_thread": False},
                           echo_pool=True,  # 풀 이벤트 로그 활성화
                           pool_pre_ping=True,  # 끊어진 커넥션 감지 권장
                           )
    return engine


def get_engine(db_type: str, host, port, username, password, db_name, schema=None):
    if db_type == "postgresql":
        return create_postgres_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
            f"?application_name=meta-workflow-server",
            schema
        )
    elif db_type == "sqlite":
        return create_sqlite_engine(
            f"sqlite:///{db_name}",
        )
    else:
        raise HTTPException(status_code=404, detail="Invalid database URI")


SessionLocalBaseDB = scoped_session(
    sessionmaker(autocommit=False,
                 autoflush=False,
                 bind=get_engine(Config.DB_TYPE,
                                 Config.DB_HOST,
                                 Config.DB_PORT,
                                 Config.DB_USERNAME,
                                 Config.DB_PASSWORD,
                                 Config.DB_NAME,
                                 Config.DB_SCHEMA,
                                 )
                 ))
SessionLocalAirflowDB = scoped_session(
    sessionmaker(autocommit=False,
                 autoflush=False,
                 bind=get_engine(Config.AIRFLOW_DB_TYPE,
                                 Config.AIRFLOW_DB_HOST,
                                 Config.AIRFLOW_DB_PORT,
                                 Config.AIRFLOW_DB_USERNAME,
                                 Config.AIRFLOW_DB_PASSWORD,
                                 Config.AIRFLOW_DB_NAME,
                                 Config.AIRFLOW_DB_SCHEMA,
                                 ),
                 class_=ReadOnlySession
                 ))
if Config.DB_TYPE == "postgresql":
    BaseDB = declarative_base(metadata=MetaData(schema=Config.DB_SCHEMA))
    AirflowDB = declarative_base(metadata=MetaData(schema=Config.AIRFLOW_DB_SCHEMA))
else:
    BaseDB = declarative_base()
    AirflowDB = declarative_base()


# ✅ `models/` 폴더 내 모든 `.py` 파일을 자동으로 import하여 Alembic이 감지할 수 있도록 설정
def import_models():
    models_path = os.path.join(os.path.dirname(__file__), "../models/db")
    for _, module_name, _ in pkgutil.iter_modules([models_path]):
        importlib.import_module(f"models.db.{module_name}")


# 자동 모델 감지 실행
import_models()


def get_db():
    with get_db_context() as db:
        yield db


@contextmanager
def get_db_context():
    logger.info("🔓 Open Meta DB Session")
    db = SessionLocalBaseDB()
    try:
        yield db
    except:
        db.rollback()
        raise
    finally:
        db.close()
        logger.info("🔒 Close Meta DB Session")


def get_airflow():
    logger.info("🔓 Open Airflow DB Session")
    db = SessionLocalAirflowDB()
    try:
        yield db
    except:
        db.rollback()
        raise
    finally:
        db.close()
        logger.info("🔒 Close Airflow DB Session")
