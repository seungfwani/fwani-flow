import logging
import os
from logging.handlers import RotatingFileHandler

from config import Config

# 로그 디렉토리 생성 (없으면 생성)
os.makedirs(Config.LOG_DIR, exist_ok=True)

# 로그 파일 경로
LOG_FILE = os.path.join(Config.LOG_DIR, "app.log")

# 로그 포맷 설정
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

#  Uvicorn 로그 설정
LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": LOG_FORMAT,
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "[%(asctime)s] %(levelname)s, %(process)s-%(thread)d, %(message)s",
            "use_colors": None,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "access",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": LOG_FILE,
            "maxBytes": 5 * 1024 * 1024,  # 5MB
            "backupCount": 1,
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"]
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["console", "file"],
            "level": Config.LOG_LEVEL,
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["console", "file"],
            "level": Config.LOG_LEVEL,
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["console"],
            "level": Config.LOG_LEVEL,
            "propagate": False,
        },
    }
}


def setup_logging():
    logger = logging.getLogger()  # ✅ 통일된 로거 사용
    logger.setLevel(logging.getLevelName(Config.LOG_LEVEL))

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    # 콘솔 핸들러 (터미널 출력용)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 파일 핸들러 (로그 파일 저장용)
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)  # 최대 5MB, 5개 유지
    file_handler.setFormatter(formatter)

    # 로깅 설정 적용
    # dictConfig(LOG_CONFIG)
    # logger.propagate = False

    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # logging.basicConfig(
    #     level=Config.LOG_LEVEL,
    #     format=LOG_FORMAT,
    #     datefmt=DATE_FORMAT,
    #     handlers=[
    #         console_handler,
    #         file_handler,
    #     ],
    # )

    logging.info("✅ 로깅 시스템 초기화 완료")


# 실행 시 자동 설정 적용
setup_logging()
