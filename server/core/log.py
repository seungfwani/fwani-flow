import logging
import os
from logging.handlers import RotatingFileHandler

from config import Config

# 로그 디렉토리 생성 (없으면 생성)
LOG_DIR = Config.LOG_DIR
os.makedirs(LOG_DIR, exist_ok=True)

# 로그 파일 경로
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# 로그 포맷 설정
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 로거 생성
logger = logging.getLogger("app_logger")
logger.setLevel(logging.INFO)  # 기본 레벨 설정

# 콘솔 핸들러 (터미널 출력용)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

# 파일 핸들러 (로그 파일 저장용)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)  # 최대 5MB, 5개 유지
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

# 핸들러 추가
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ✅ 로그 출력 테스트
logger.info("✅ 로거 설정 완료: FastAPI 실행 준비 완료")
