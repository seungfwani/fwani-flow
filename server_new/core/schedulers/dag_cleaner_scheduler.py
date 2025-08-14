import logging
import shutil
from pathlib import Path

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocalBaseDB
from models.db.flow import Flow

logger = logging.getLogger()


def clean_orphan_dag_files(db: Session):
    logger.info("🔄 DAG 디렉토리 정리 시작.")
    base_path = Path(Config.DAG_DIR)
    for folder in base_path.glob("dag_*"):
        if folder.is_dir():
            print(f"확인 할 폴더: {folder}")
            dag_name = folder.name
            flow = db.query(Flow).filter(Flow.dag_id.like(f"{dag_name}%")).first()
            if flow is None:
                shutil.rmtree(folder)
                logger.info(f"🧹 관리되지 않는 DAG 디렉토리 삭제: {folder}")
    logger.info("✅ DAG 디렉토리 정리 완료.")


def dag_cleaner_job():
    db = SessionLocalBaseDB()
    try:
        clean_orphan_dag_files(db)
    finally:
        db.close()
