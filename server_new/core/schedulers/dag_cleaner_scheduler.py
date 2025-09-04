import logging
import shutil
from pathlib import Path

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocalBaseDB
from core.snapshot import SnapshotOperation
from models.db.flow import Flow, FlowSnapshot

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

def clean_dummy_dags(db: Session):
    logger.info("▶️ Start to clean Dummy DAGs")
    db.query(FlowSnapshot).filter(FlowSnapshot.version == 1,
                                  FlowSnapshot.op == SnapshotOperation.CREATE.name,
                                  FlowSnapshot.message == Config.DUMMY_MSG,
                                  ).delete()
    logger.info("✅ Complete to clean Dummy DAGs")

def dag_cleaner_job():
    db = SessionLocalBaseDB()
    try:
        clean_orphan_dag_files(db)
        clean_dummy_dags(db)
    finally:
        db.close()
