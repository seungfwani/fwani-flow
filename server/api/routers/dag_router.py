import base64
import json
import logging
import os.path
import pickle
import traceback
from typing import List

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, DAGResponse
from api.render_template import render_dag_script
from config import Config
from core.database import get_db
from models.edge import Edge
from models.edge_ui import EdgeUI
from models.flow import Flow
from models.function_library import FunctionLibrary
from models.task import Task
from models.task_input import TaskInput
from models.task_ui import TaskUI
from utils.airflow_client import get_airflow_client, AirflowClient
from utils.udf_validator import get_validated_inputs

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag"],
)


def make_flow(dag: DAGRequest, dag_id: str, udf_functions: {str, FunctionLibrary}):
    # Flow 생성
    flow = Flow(id=dag_id, name=dag.name, description=dag.description)

    # tasks 생성
    id_to_variable_id = {}
    for i, node in enumerate(dag.nodes):
        variable_id = f"task_{i}"
        id_to_variable_id[node.id] = variable_id
    tasks = {}
    for i, node in enumerate(dag.nodes):
        # 첫 번째 노드인지 확인
        is_first_task = all(edge.target != node.id for edge in dag.edges)

        task_inputs = get_validated_inputs(udf_functions[node.data.function_id].inputs, node.data.inputs)
        if not is_first_task:
            # 부모 노드를 찾아서 before_task_id 설정
            task_inputs.append({
                "key": "before_task_ids",
                "value": json.dumps([id_to_variable_id[edge.source] for edge in dag.edges if edge.target == node.id]),
                "type": "list"
            })
        task_data = Task(
            variable_id=id_to_variable_id[node.id],
            function_id=node.data.function_id,
            decorator="file_decorator",
        )
        for inp in task_inputs:
            task_data.inputs.append(TaskInput(
                key=inp.get("key"),
                type=inp.get("type"),
                value=inp.get("value"),
            ))
        logger.info(f"Task Data: {task_data}")
        task_data.task_ui = TaskUI(type=node.type,
                                   position=node.position,
                                   style=node.style, )
        flow.add_task(task_data)
        tasks[node.id] = task_data

    # edge 생성
    for edge in dag.edges:
        edge_data = Edge(
            from_task=tasks[edge.source],
            to_task=tasks[edge.target]
        )
        edge_data.edge_ui = EdgeUI(
            type=edge.type,
            label=edge.label,
            labelStyle=edge.labelStyle,
            labelBgStyle=edge.labelBgStyle,
            labelBgPadding=edge.labelBgPadding,
            labelBgBorderRadius=edge.labelBgBorderRadius,
            style=edge.style,
        )
        flow.add_edge(edge_data)
    return flow


def create_dag_by_id(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
    try:
        udf_functions: {str, FunctionLibrary} = {
            udf.id: udf
            for udf in db.query(FunctionLibrary)
            .filter(FunctionLibrary.id.in_([node.data.function_id for node in dag.nodes]))
            .all()
        }
        # find missing udf
        missing_udfs = [node for node in dag.nodes
                        if node.data.function_id not in udf_functions.keys()]
        if missing_udfs:
            logger.error(f"UDFs not found: {missing_udfs}")
            return {"message": f"UDFs not found: {missing_udfs}"}

        # make/save dag metadata to DB
        flow = make_flow(dag, dag_id, udf_functions)
        db.add(flow)
        db.flush()

        # write dag
        with open(dag_file_path, 'w') as dag_file:
            dag_file.write(render_dag_script(dag_id, flow.tasks, flow.edges))
        db.commit()
        return DAGResponse.from_dag(flow)
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        logger.error(traceback.format_exc())
        db.rollback()
        logger.warning(f"🔄 메타데이터 롤백")

        # ✅ 파일 저장 후 DB 실패 시 파일 삭제
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


def delete_dag_metadata(dag_id: str, db: Session):
    logger.info("Get DAG {dag_id} metadata for deleting")
    flow = db.query(Flow).filter(Flow.id == dag_id).first()
    if not flow:
        raise ValueError(f"DAG {dag_id} not found")

    db.query(Edge).filter(Edge.flow_id == flow.id).delete()
    db.query(Task).filter(Task.flow_id == flow.id).delete()
    db.delete(flow)
    logger.info("Delete DAG {dag_id} metadata")
    return flow


def get_flow(dag_id: str, db: Session = Depends(get_db)):
    flow = db.query(Flow).filter(Flow.id == dag_id).first()
    if not flow:
        raise ValueError(f"DAG {dag_id} not found")
    return flow


@router.post("",
             response_model=APIResponse[DAGResponse],
             )
@api_response_wrapper
async def create_dag(dag: DAGRequest, db: Session = Depends(get_db)):
    """DAG 생성 및 DB 에 저장"""
    logger.info(f"Request Data: {dag}")
    dag_id = "dag_" + base64.urlsafe_b64encode(dag.name.encode()).rstrip(b'=').decode('ascii')
    return create_dag_by_id(dag_id, dag, db)


@router.patch("/{dag_id}",
              response_model=APIResponse[DAGResponse],
              )
@api_response_wrapper
async def update_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    try:
        dag_data = delete_dag_metadata(dag_id, db)
        logger.info(f"Delete DAG metadata {dag_data}")
        created_dag = create_dag_by_id(dag_id, dag, db)
        db.commit()
        logger.info(f"✅ Success to update DAG : {created_dag}")
        return created_dag
    except Exception as e:
        db.rollback()
        logger.error(f"DAG 업데이트 실패: {e}")
        raise


@router.delete("/{dag_id}",
               response_model=APIResponse[DAGResponse],
               )
@api_response_wrapper
async def delete_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Delete a python DAG file
    :param dag_id:
    :param db:
    :return:
    """
    dag_data = delete_dag_metadata(dag_id, db)
    db.commit()

    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
    if os.path.exists(dag_file_path):
        try:
            os.remove(dag_file_path)
            logger.warning(f"🗑️ 저장된 DAG 파일 삭제: {dag_file_path}")
        except Exception as e:
            logger.error(f"파일 삭제 실패: {e}")
            # 파일 삭제 실패는 치명적이지 않으니 경고만 로그 남기고 넘어갈 수 있음
    return DAGResponse.from_dag(dag_data)


@router.get("",
            response_model=APIResponse[List[DAGResponse]],
            )
@api_response_wrapper
async def get_dag_list(db: Session = Depends(get_db)):
    """
    Get all available DAG
    :return:
    """
    logger.info(f"▶️ DAG 리스트 조회")
    return [DAGResponse.from_dag(dag) for dag in db.query(Flow).all()]


@router.get("/{dag_id}",
            response_model=APIResponse[DAGResponse],
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Get DAG list
    :return:
    """
    logger.info(f"Get DAG {dag_id}")
    return DAGResponse.from_dag(db.query(Flow).filter(Flow.id == dag_id).first())


@router.post("/{dag_id}/trigger")
@api_response_wrapper
async def get_dag_runs(dag_id: str, airflow_client: AirflowClient = Depends(get_airflow_client)):
    """
    Run DAG
    :param dag_id:
    :param airflow_client:
    :return:
    """
    response = airflow_client.post(f"dags/{dag_id}/dagRuns", json_data=json.dumps({}))
    logger.info(response)
    return response


@router.patch("/{dag_id}/kill/{dag_run_id}")
@api_response_wrapper
async def kill_dag_run(dag_id: str, dag_run_id: str, airflow_client: AirflowClient = Depends(get_airflow_client)):
    """
    kill job in DAG
    :param dag_id:
    :param dag_run_id:
    :param airflow_client:
    :return:
    """
    response = airflow_client.patch(f"dags/{dag_id}/dagRuns/{dag_run_id}", json_data=json.dumps({
        "state": "failed",
    }))
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}")
@api_response_wrapper
async def get_dag_run(dag_id: str, dag_run_id: str, airflow_client: AirflowClient = Depends(get_airflow_client)):
    """
    get job in DAG
    :param dag_id:
    :param dag_run_id:
    :param airflow_client:
    :return:
    """
    response = airflow_client.get(f"dags/{dag_id}/dagRuns/{dag_run_id}")
    logger.info(response)
    return response


@router.get("/{dag_id}/history")
@api_response_wrapper
async def get_history_of_dag(dag_id: str, airflow_client: AirflowClient = Depends(get_airflow_client)):
    """
    get job history of DAG
    :param dag_id:
    :param airflow_client:
    :return:
    """
    response = airflow_client.get(f"dags/{dag_id}/dagRuns")
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}/tasks/{task_id}")
@api_response_wrapper
async def get_task_of_dag_run(dag_id: str, dag_run_id: str, task_id: str,
                              airflow_client: AirflowClient = Depends(get_airflow_client),
                              flow: Flow = Depends(get_flow)):
    """
    get job history of DAG
    :param task_id:
    :param dag_run_id:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    task_variable_id = None
    for task in flow.tasks:
        if task.task_id == task_id:
            task_variable_id = task.variable_id
            break
    if task_variable_id is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    response = airflow_client.get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_variable_id}")
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}/result")
@api_response_wrapper
async def get_task_of_dag_run(dag_id: str, dag_run_id: str):
    """
    get job history of DAG
    :param dag_run_id:
    :param dag_id:
    :return:
    """
    return get_dag_result(dag_id, dag_run_id)


def get_dag_result(dag_id, run_id):
    shared_dir = os.path.abspath(Config.SHARED_DIR)
    result_dir = os.path.join(shared_dir, f"dag_id={dag_id}/run_id={run_id}")
    json_path = os.path.join(result_dir, "final_result.json")
    pkl_path = os.path.join(result_dir, "final_result.pkl")
    if os.path.exists(json_path):
        logger.info(f"load json file: {json_path}")
        with open(json_path, "r") as f:
            return json.load(f)
    elif os.path.exists(pkl_path):
        try:
            logger.info(f"load pickle file: {pkl_path}")
            with open(pkl_path, "rb") as f:
                result = pickle.load(f)
            return {"result": str(result), "type": "pickle"}
        except Exception as e:
            logger.error("⚠️ Failed to load pickle result", e)
            raise
    else:
        raise HTTPException(status_code=404, detail="결과 파일이 존재하지 않습니다.")
