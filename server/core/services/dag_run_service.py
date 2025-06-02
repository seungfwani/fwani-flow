import json
import logging
import os
import pickle
from typing import Tuple, List

from fastapi import HTTPException
from sqlalchemy.orm import Session

from config import Config
from models.airflow_dag_run_history import AirflowDagRunHistory, AirflowDagRunSnapshotTask
from models.function_library import FunctionLibrary
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
    logger.info(f"airflow_client taskInstance response: {response}")
    task_mapper = {t.variable_id: t for t in flow_run.snapshot_tasks}
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
    task_mapper = {t.variable_id: t for t in flow_run.snapshot_tasks}
    logger.info(f"task information for the current DAG: {task_mapper}")

    task_variable_id = response['task_id']
    if task_variable_id in task_mapper:
        response['task_id'] = task_mapper[task_variable_id].task_id
        return task_mapper[task_variable_id], response
    else:
        raise ValueError(f"Task {task_id} not found")


def get_snapshot_task_by_id(task_id: str, db: Session) -> (AirflowDagRunSnapshotTask, FunctionLibrary):
    task: AirflowDagRunSnapshotTask = (db.query(AirflowDagRunSnapshotTask)
                                       .filter(AirflowDagRunSnapshotTask.task_id == task_id)
                                       .first())
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task({task_id}) 가 존재하지 않습니다.")

    function_ = db.query(FunctionLibrary).filter(FunctionLibrary.id == task.function_id).first()
    return task, function_


def get_task_result_each_tasks(run_id: str, task_id: str, db: Session):
    flow_run = get_flow_run_history(run_id, db)
    task, function_ = get_snapshot_task_by_id(task_id, db)

    airflow_dag_id = flow_run.dag_id
    airflow_run_id = flow_run.run_id

    shared_dir = os.path.abspath(Config.SHARED_DIR)
    result_dir = os.path.join(shared_dir, f"dag_id={airflow_dag_id}/run_id={airflow_run_id}")
    pkl_path = os.path.join(result_dir, f"{task.variable_id}.pkl")
    if os.path.exists(pkl_path):
        try:
            logger.info(f"load pickle file: {pkl_path}")
            with open(pkl_path, "rb") as f:
                result = pickle.load(f)
            return {"result": result, "type": function_.output.type}
        except Exception as e:
            logger.error("⚠️ Failed to load pickle result", e)
            raise
    else:
        raise HTTPException(status_code=404, detail="결과 파일이 존재하지 않습니다.")


def get_task_logs(run_id: str, task_id: str, try_number: int, airflow_client: AirflowClient, db: Session) -> str:
    flow_run = get_flow_run_history(run_id, db)
    task, function_ = get_snapshot_task_by_id(task_id, db)
    airflow_dag_id = flow_run.dag_id
    airflow_run_id = flow_run.run_id
    airflow_task_id = task.variable_id

    return airflow_client.get(
        f"dags/{airflow_dag_id}/dagRuns/{airflow_run_id}/taskInstances/{airflow_task_id}/logs/{try_number}")
