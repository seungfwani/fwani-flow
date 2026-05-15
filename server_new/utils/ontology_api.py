"""Ontology meta-type API URL (Config ONTOLOGY_* / ENDPOINT_DAG_*)."""

from config import Config

META_TYPE_API_PREFIX = "/graphio/v1/meta-type/"


def get_ontology_base_url() -> str:
    return f"http://{Config.ONTOLOGY_HOST}:{Config.ONTOLOGY_PORT}"


def get_workflow_dag_save_url() -> str:
    endpoint = Config.ENDPOINT_DAG_SAVE.lstrip("/")
    return f"{get_ontology_base_url().rstrip('/')}{META_TYPE_API_PREFIX}{endpoint}"


def get_workflow_dag_run_url() -> str:
    endpoint = Config.ENDPOINT_DAG_RUN.lstrip("/")
    return f"{get_ontology_base_url().rstrip('/')}{META_TYPE_API_PREFIX}{endpoint}"
