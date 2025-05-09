import os


class Config:
    # 기본 설정
    DEBUG = os.getenv("DEBUG", "True").lower() == "true"
    TESTING = os.getenv("TESTING", "False").lower() == "true"
    SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-key")
    AUTH_PROVIDER = os.getenv("AUTH_PROVIDER", "local")

    # 데이터베이스 설정
    DB_ENGINE = os.getenv("DB_ENGINE", "sqlite")
    DB_NAME = os.getenv("DB_NAME", "workflow.db")
    DB_URI = os.getenv("DB_URI", f"sqlite:///{DB_NAME}")

    # Docker 설정
    DOCKER_HOST = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock")
    DOCKER_API_VERSION = os.getenv("DOCKER_API_VERSION", "1.41")

    # 로그 경로
    LOG_DIR = os.getenv("LOG_DIR", "./data/logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")

    WORKFLOW_DIR = os.getenv("WORKFLOW_DIR", "./data/workflows")
    UDF_DIR = os.getenv("UDF_DIR", "./data/udfs")
    DAG_DIR = os.getenv("DAG_DIR", "./data/dags")
    SHARED_DIR = os.getenv("SHARED_DIR", "./data/shared")

    # airflow
    AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "localhost")
    AIRFLOW_PORT = os.getenv("AIRFLOW_PORT", "8080")
    AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
    AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "<PASSWORD>")
