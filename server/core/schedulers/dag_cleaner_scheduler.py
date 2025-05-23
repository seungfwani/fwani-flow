import glob
import logging
import os

from sqlalchemy.orm import Session

from config import Config
from core.database import SessionLocal
from models.flow_version import FlowVersion

logger = logging.getLogger()


def clean_orphan_dag_files(db: Session):
    logger.info("🔄 DAG 디렉토리 정리 시작.")
    all_files = glob.glob(os.path.join(Config.DAG_DIR, "**/*.py"))
    for f in all_files:
        dag_version_of_file = f.rsplit("/", maxsplit=2)[-2:]
        if len(dag_version_of_file) != 2:
            continue
        version = dag_version_of_file[1].removeprefix("v").removesuffix(".py")
        if version.startswith("draft"):
            result = db.query(FlowVersion).filter(FlowVersion.flow_id == dag_version_of_file[0],
                                                  FlowVersion.is_draft == True
                                                  ).first()
        else:
            result = db.query(FlowVersion).filter(FlowVersion.flow_id == dag_version_of_file[0],
                                                  FlowVersion.version == int(version)
                                                  ).first()
        if result is None:
            try:
                logger.info(f"🧹 Removed unrecognized DAG: {f}")
                os.remove(f)
                # 상위 디렉토리가 비었는지 확인 후 삭제
                parent_dir = os.path.dirname(f)
                if not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                    logger.info(f"🧹 Removed empty directory: {parent_dir}")
            except Exception:
                pass
    logger.info("✅ DAG 디렉토리 정리 완료.")


def dag_cleaner_job():
    db = SessionLocal()
    try:
        clean_orphan_dag_files(db)
    finally:
        db.close()
