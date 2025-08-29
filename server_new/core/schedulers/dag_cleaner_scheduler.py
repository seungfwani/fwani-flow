import logging
import shutil
from pathlib import Path

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocalBaseDB
from models.db.flow import Flow

logger = logging.getLogger()


def clean_orphan_dag_files(db: Session):
    logger.info("▶️ Start to clean DAG directory")
    base_path = Path(Config.DAG_DIR)
    for folder in base_path.glob("dag_*"):
        if folder.is_dir():
            logger.info(f"📁 Directory to check: {folder}")
            dag_name = folder.name.split("__")[0]
            flow = db.query(Flow).filter(Flow.dag_id == dag_name).first()
            if flow is None:
                shutil.rmtree(folder)
                logger.info(f"🧹 Delete unmanaged DAG directory: {folder}")
    logger.info("✅ Complete to clean DAG directory")


def dag_cleaner_job():
    db = SessionLocalBaseDB()
    try:
        clean_orphan_dag_files(db)
    finally:
        db.close()
