import json
import logging
from typing import Tuple, List

from sqlalchemy.orm import Session

from models.airflow_dag_run_history import AirflowDagRunHistory, AirflowDagRunSnapshotTask
from utils.airflow_client import AirflowClient

logger = logging.getLogger()


def get_flow_run_history(run_id: str, db: Session) -> AirflowDagRunHistory:
    return db.query(AirflowDagRunHistory).filter(
        AirflowDagRunHistory.id == run_id,
    ).first()


def kill_flow_run(run_id: str, airflow_client: AirflowClient, db: Session):
    flow_run = get_flow_run_history(run_id, db)
    response = airflow_client.patch(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}",
                                    json_data=json.dumps({
                                        "state": "failed",
                                    }))
    logger.info(response)
    return response


def get_all_tasks_by_run_id(run_id: str, airflow_client: AirflowClient, db: Session) \
        -> Tuple[AirflowDagRunHistory, List[Tuple[AirflowDagRunSnapshotTask, dict]]]:
    flow_run = get_flow_run_history(run_id, db)
    response = airflow_client.get(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}/taskInstances")
    flow_run_snapshot_tasks = {t.task_id: t for t in flow_run.snapshot_tasks}
    logger.info(f"airflow_client taskInstance response: {response}")
    task_mapper = {t.variable_id: flow_run_snapshot_tasks[t.id] for t in flow_run.flow_version.tasks}
    logger.info(f"task information for the current DAG: {task_mapper}")

    task_instance_data = []
    for ti in response.get("task_instances", []):
        task_variable_id = ti['task_id']
        if task_variable_id in task_mapper:
            ti['task_id'] = task_mapper[task_variable_id].task_id
            task_instance_data.append((task_mapper[task_variable_id], ti))
    return flow_run, task_instance_data


def get_task_in_run_id(run_id: str, task_id: str, airflow_client: AirflowClient, db: Session):
    flow_run = get_flow_run_history(run_id, db)
    airflow_task_id = None
    for task in flow_run.flow_version.tasks:
        if task.id == task_id:
            airflow_task_id = task.variable_id
            break
    if airflow_task_id is None:
        raise ValueError(f"Task {task_id} not found")

    response = airflow_client.get(f"dags/{flow_run.dag_id}/dagRuns/{flow_run.run_id}/taskInstances/{airflow_task_id}")
    logger.info(f"airflow_client taskInstance response: {response}")
    flow_run_snapshot_tasks = {t.task_id: t for t in flow_run.snapshot_tasks}
    task_mapper = {t.variable_id: flow_run_snapshot_tasks[t.id] for t in flow_run.flow_version.tasks}
    logger.info(f"task information for the current DAG: {task_mapper}")

    task_variable_id = response['task_id']
    if task_variable_id in task_mapper:
        response['task_id'] = task_mapper[task_variable_id].task_id
        return task_mapper[task_variable_id], response
    else:
        raise ValueError(f"Task {task_id} not found")
