import os


class Config:
    # 기본 설정
    DEBUG = os.getenv("DEBUG", "True").lower() == "true"
    TESTING = os.getenv("TESTING", "False").lower() == "true"
    SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-key")
    AUTH_PROVIDER = os.getenv("AUTH_PROVIDER", "local")

    # 데이터베이스 설정
    DB_ENGINE = os.getenv("DB_ENGINE", "sqlite")
    DB_TYPE = os.getenv("DB_TYPE", "postgresql")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "65432")
    DB_NAME = os.getenv("DB_NAME", "graphio")
    DB_SCHEMA = os.getenv("DB_SCHEMA", "workflow")
    DB_USERNAME = os.getenv("DB_USERNAME", "workflow")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "<PASSWORD>")
    AIRFLOW_DB_TYPE = os.getenv("AIRFLOW_DB_TYPE", "postgresql")
    AIRFLOW_DB_HOST = os.getenv("AIRFLOW_DB_HOST", "airflow-svc")
    AIRFLOW_DB_PORT = os.getenv("AIRFLOW_DB_PORT", "8080")
    AIRFLOW_DB_NAME = os.getenv("AIRFLOW_DB_NAME", "graphio")
    AIRFLOW_DB_SCHEMA = os.getenv("AIRFLOW_DB_SCHEMA", "airflow")
    AIRFLOW_DB_USERNAME = os.getenv("AIRFLOW_DB_USERNAME", "airflow")
    AIRFLOW_DB_PASSWORD = os.getenv("AIRFLOW_DB_PASSWORD", "<PASSWORD>")

    # 로그 경로
    LOG_DIR = os.getenv("LOG_DIR", "./data/logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")

    UDF_DIR = os.getenv("UDF_DIR", "../server/data/udfs")
    BUILTIN_FUNC_SCRIPT_DIR = os.getenv("BUILTIN_FUNC_SCRIPT_DIR", "../builtin_functions")
    DAG_DIR = os.getenv("DAG_DIR", "../server/data/dags")
    SHARED_DIR = os.getenv("SHARED_DIR", "../server/data/shared")

    # airflow
    AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "localhost")
    AIRFLOW_PORT = os.getenv("AIRFLOW_PORT", "8080")
    AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
    AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "<PASSWORD>")
