# sync_system_functions.py
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml

from config import Config
from core.database import get_db_context
from models.db.system_function import SystemFunction

SYSTEM_FUNCTION_CONFIG_DIR = Path(Config.BUILTIN_FUNC_CONFIG_DIR)

logger = logging.getLogger()

def load_system_functions_from_yaml():
    functions = []
    for yaml_file in SYSTEM_FUNCTION_CONFIG_DIR.glob("*.yml"):
        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            functions.append(data)
    return functions


def sync_system_functions_from_yaml():
    logger.info("Syncing system functions...")
    catalog = load_system_functions_from_yaml()
    logger.info(f"Found {len(catalog)} system functions")
    now = datetime.now(timezone.utc)
    seen_ids = set()

    with get_db_context() as db:
        for row in catalog:
            logger.info(f"Processing {row['name']}")
            seen_ids.add(row["id"])
            row["updated_at"] = now
            existing = db.get(SystemFunction, row["id"])
            if existing:
                for k, v in row.items():
                    setattr(existing, k, v)
            else:
                db.add(SystemFunction(**row))
        deprecated_sfs = db.query(SystemFunction).filter(~SystemFunction.id.in_(seen_ids)).all()
        for sf in deprecated_sfs:
            sf.is_deprecated = True
            sf.updated_at = now
        db.commit()


if __name__ == "__main__":
    functions = load_system_functions_from_yaml()
    print(functions)
