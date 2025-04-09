import base64
import datetime
import hashlib
import json
import logging
import os.path
import pickle
import traceback
import uuid
from typing import List, Any

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, DAGResponse, DAGNode
from api.render_template import render_dag_script
from config import Config
from core.database import get_db
from models.edge import Edge
from models.edge_ui import EdgeUI
from models.flow import Flow
from models.flow_version import FlowVersion
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


# def make_flow(dag: DAGRequest, dag_id: str, udf_functions: {str, FunctionLibrary}):
#     # Flow 생성
#     flow = Flow(id=dag_id, name=dag.name, description=dag.description)
#
#     # tasks 생성
#     id_to_variable_id = {}
#     for i, node in enumerate(dag.nodes):
#         variable_id = f"task_{i}"
#         id_to_variable_id[node.id] = variable_id
#     tasks = {}
#     for i, node in enumerate(dag.nodes):
#         # 첫 번째 노드인지 확인
#         is_first_task = all(edge.target != node.id for edge in dag.edges)
#
#         task_inputs = get_validated_inputs(udf_functions[node.data.function_id].inputs, node.data.inputs)
#         if not is_first_task:
#             # 부모 노드를 찾아서 before_task_id 설정
#             task_inputs.append({
#                 "key": "before_task_ids",
#                 "value": json.dumps([id_to_variable_id[edge.source] for edge in dag.edges if edge.target == node.id]),
#                 "type": "list"
#             })
#         task_data = Task(
#             variable_id=id_to_variable_id[node.id],
#             function_id=node.data.function_id,
#             decorator="file_decorator",
#         )
#         for inp in task_inputs:
#             task_data.inputs.append(TaskInput(
#                 key=inp.get("key"),
#                 type=inp.get("type"),
#                 value=inp.get("value"),
#             ))
#         logger.info(f"Task Data: {task_data}")
#         task_data.task_ui = TaskUI(type=node.type,
#                                    position=node.position,
#                                    style=node.style, )
#         flow.add_task(task_data)
#         tasks[node.id] = task_data
#
#     # edge 생성
#     for edge in dag.edges:
#         edge_data = Edge(
#             from_task=tasks[edge.source],
#             to_task=tasks[edge.target]
#         )
#         edge_data.edge_ui = EdgeUI(
#             type=edge.type,
#             label=edge.label,
#             labelStyle=edge.labelStyle,
#             labelBgStyle=edge.labelBgStyle,
#             labelBgPadding=edge.labelBgPadding,
#             labelBgBorderRadius=edge.labelBgBorderRadius,
#             style=edge.style,
#         )
#         flow.add_edge(edge_data)
#     return flow
#
#
# def create_dag_by_id(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
#     dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
#     try:
#         udf_functions: {str, FunctionLibrary} = {
#             udf.id: udf
#             for udf in db.query(FunctionLibrary)
#             .filter(FunctionLibrary.id.in_([node.data.function_id for node in dag.nodes]))
#             .all()
#         }
#         # find missing udf
#         missing_udfs = [node for node in dag.nodes
#                         if node.data.function_id not in udf_functions.keys()]
#         if missing_udfs:
#             logger.error(f"UDFs not found: {missing_udfs}")
#             return {"message": f"UDFs not found: {missing_udfs}"}
#
#         # make/save dag metadata to DB
#         flow = make_flow(dag, dag_id, udf_functions)
#         db.add(flow)
#         db.flush()
#
#         # write dag
#         with open(dag_file_path, 'w') as dag_file:
#             dag_file.write(render_dag_script(dag_id, flow.tasks, flow.edges))
#         db.commit()
#         return DAGResponse.from_dag(flow)
#     except Exception as e:
#         logger.error(f"❌ 오류 발생: {e}")
#         logger.error(traceback.format_exc())
#         db.rollback()
#         logger.warning(f"🔄 메타데이터 롤백")
#
#         # ✅ 파일 저장 후 DB 실패 시 파일 삭제
#         if os.path.exists(dag_file_path):
#             os.remove(dag_file_path)
#             logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
#         raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


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


def get_flow(dag_id: str, db: Session):
    flow = db.query(Flow).filter(Flow.id == dag_id).first()
    if not flow:
        raise ValueError(f"DAG {dag_id} not found")
    return flow


def make_dag_id_by_name(name: str) -> str:
    return "dag_" + base64.urlsafe_b64encode(name.encode()).rstrip(b'=').decode('ascii')


def get_udf_functions(dag: DAGRequest, db: Session) -> dict:
    udf_functions: dict[str, FunctionLibrary] = {
        udf.id: udf
        for udf in db.query(FunctionLibrary)
        .filter(FunctionLibrary.id.in_([node.data.function_id for node in dag.nodes]))
        .all()
    }

    # check missing UDFs
    missing_udfs = [node for node in dag.nodes if node.data.function_id not in udf_functions]
    if missing_udfs:
        logger.error(f"UDFs not found: {missing_udfs}")
        raise HTTPException(status_code=400, detail=f"UDFs not found: {missing_udfs}")

    return udf_functions


def create_tasks(dag: DAGRequest, db: Session) -> dict:
    # Create Tasks
    udf_functions = get_udf_functions(dag, db)
    id_to_variable_id = {}
    for i, node in enumerate(dag.nodes):
        variable_id = f"task_{i}"
        id_to_variable_id[node.id] = variable_id

    tasks = {}
    for i, node in enumerate(dag.nodes):
        is_first_task = all(edge.target != node.id for edge in dag.edges)
        task_inputs = get_validated_inputs(udf_functions[node.data.function_id].inputs, node.data.inputs)
        if not is_first_task:
            task_inputs.append({
                "key": "before_task_ids",
                "value": json.dumps([id_to_variable_id[edge.source] for edge in dag.edges if edge.target == node.id]),
                "type": "list"
            })

        task = Task(
            variable_id=id_to_variable_id[node.id],
            function=udf_functions[node.data.function_id],
            decorator="file_decorator",
        )
        for inp in task_inputs:
            task.inputs.append(TaskInput(
                key=inp.get("key"),
                type=inp.get("type"),
                value=inp.get("value"),
            ))
        task.task_ui = TaskUI(type=node.type, position=node.position, style=node.style)
        tasks[node.id] = task
    return tasks


def create_edges(dag: DAGRequest, tasks: dict) -> list:
    # Create Edges
    edges = []
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
        edges.append(edge_data)
    return edges


def create_flow(dag: DAGRequest):
    dag_id = make_dag_id_by_name(dag.name)
    return Flow(
        id=dag_id,
        name=dag.name,
        description=dag.description,
        owner=dag.owner,
    )


def create_draft_version(dag: DAGRequest, flow: Flow, db: Session, version=1) -> FlowVersion:
    # Create FlowVersion as draft
    version = FlowVersion(id=str(uuid.uuid4()), flow_id=flow.id, is_draft=True, version=version, flow=flow)

    tasks = create_tasks(dag, db)
    edges = create_edges(dag, tasks)

    version.set_tasks(list(tasks.values()))
    version.set_edges(edges)
    db.add(version)
    return version


def normalize_dag(dag: DAGRequest) -> dict[str, Any]:
    def normalize_node(node: DAGNode):
        return {
            "id": node.id,
            "function_id": node.data.function_id,
            "inputs": sorted(node.data.inputs.items()),
        }

    def normalize_edge(edge):
        return edge.source, edge.target

    return {
        "name": dag.name,
        "nodes": sorted([normalize_node(n) for n in dag.nodes], key=lambda x: x["id"]),
        "edges": sorted([normalize_edge(e) for e in dag.edges], key=lambda x: (x[0], x[1]))
    }


def calculate_dag_hash(dag: DAGRequest) -> str:
    hash_input = json.dumps(normalize_dag(dag), sort_keys=True)
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


def update_draft_version(version: FlowVersion, dag: DAGRequest, db: Session) -> FlowVersion:
    # 1. 해당 draft의 task, edge 모두 삭제
    db.query(Task).filter(Task.flow_version_id == version.id).delete()
    db.query(Edge).filter(Edge.flow_version_id == version.id).delete()

    # 2. 버전 메타 정보도 갱신 (예: hash, updated_at)
    version.hash = calculate_dag_hash(dag)
    version.updated_at = datetime.datetime.now(datetime.UTC)

    # 2. 새로운 node, edge 삽입
    tasks = create_tasks(dag, db)
    edges = create_edges(dag, tasks)

    version.set_tasks(list(tasks.values()))
    version.set_edges(edges)
    return version


def is_flow_changed(new_dag: DAGRequest, latest_version_id: str, db: Session) -> bool:
    old_nodes = db.query(Task).filter(Task.flow_version_id == latest_version_id).all()
    old_edges = db.query(Edge).filter(Edge.flow_version_id == latest_version_id).all()

    # 비교를 위한 정규화된 형태로 변환
    def normalize_node(n: Task): return {
        "id": n.id,
        "function_id": n.function_id,
        "inputs": sorted([(i.key, i.value) for i in n.inputs]),
    }

    normalized_new_dag = normalize_dag(new_dag)

    # 이미 존재하면 id 도 같아야함. 새로 추가된 id 는 다를 수 밖에 없음 (old 에는 존재하지 않으니까)
    old_node_set = sorted([normalize_node(n) for n in old_nodes], key=lambda x: x["id"])
    new_node_set = normalized_new_dag.get("nodes")

    if old_node_set != new_node_set:
        return True

    def normalize_edge(e: Edge): return e.from_task_id, e.to_task_id

    old_edge_set = sorted([normalize_edge(e) for e in old_edges], key=lambda x: (x[0], x[1]))
    new_edge_set = normalized_new_dag.get("edges")

    return old_edge_set != new_edge_set


def write_dag_file(flow_version: FlowVersion):
    dag_dir_path = os.path.join(Config.DAG_DIR, flow_version.flow_id)
    os.makedirs(dag_dir_path, exist_ok=True)
    dag_version = "draft" if flow_version.is_draft else f"v{flow_version.version}"
    dag_file_path = os.path.join(dag_dir_path, dag_version)
    try:
        # write dag
        with open(dag_file_path + ".py", 'w') as dag_file:
            dag_file.write(render_dag_script(f"{flow_version.flow_id}__{dag_version}",
                                             flow_version.tasks,
                                             flow_version.edges))
    except Exception as e:
        logger.error(f"❌ DAG 파일 생성 실패: {e}")
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


def create_update_draft_dag(dag: DAGRequest, db: Session) -> FlowVersion:
    dag_id = make_dag_id_by_name(dag.name)
    try:

        existing_draft = db.query(FlowVersion).filter(FlowVersion.flow_id == dag_id).first()
        if existing_draft:  # 기존 draft 가 있으므로, 수정
            if not is_flow_changed(dag, existing_draft.id, db):
                return existing_draft
            else:
                new_draft = update_draft_version(existing_draft, dag, db)
        else:  # 새 draft 생성
            flow = create_flow(dag)
            new_draft = create_draft_version(dag, flow, db)
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        logger.error(traceback.format_exc())
        db.rollback()
        logger.warning(f"🔄 메타데이터 롤백")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")
    write_dag_file(new_draft)
    db.commit()
    return new_draft


def publish_dag_version(dag_id: str, dag: DAGRequest, db: Session) -> FlowVersion:
    # 1. draft version 찾기
    draft = db.query(FlowVersion).filter_by(flow_id=dag_id, is_draft=True).first()
    if not draft:
        flow = db.query(Flow).filter_by(id=dag_id).first()
        if not flow:
            flow = create_flow(dag)
            draft = create_draft_version(dag, flow, db)
        else:
            if len(flow.versions) == 0:
                draft = create_draft_version(dag, flow, db)
            else:
                last_flow_version = sorted(flow.versions, key=lambda v: v.version, reverse=True)[0]
                draft = create_draft_version(dag, flow, db, last_flow_version.version + 1)

    # 2. draft → publish 전환
    draft.is_draft = False
    db.flush()

    write_dag_file(draft)
    db.commit()
    return draft


@router.post("",
             response_model=APIResponse[DAGResponse],
             )
@api_response_wrapper
async def draft_dag(dag: DAGRequest, db: Session = Depends(get_db)):
    """DAG draft 버전 생성 및 수정"""
    logger.info(f"Request Data: {dag}")
    return DAGResponse.from_dag(create_update_draft_dag(dag, db))  # flow version 으로 변경 필요


# @router.post("",
#              response_model=APIResponse[DAGResponse],
#              )
# @api_response_wrapper
# async def create_dag(dag: DAGRequest, db: Session = Depends(get_db)):
#     """DAG 생성 및 DB 에 저장"""
#     logger.info(f"Request Data: {dag}")
#     dag_id = "dag_" + base64.urlsafe_b64encode(dag.name.encode()).rstrip(b'=').decode('ascii')
#     return create_dag_by_id(dag_id, dag, db)


@router.patch("/{dag_id}/publish",
              response_model=APIResponse[DAGResponse],
              )
@api_response_wrapper
async def publish_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    try:
        DAGResponse.from_dag(publish_dag_version(dag_id, dag, db))
    except Exception as e:
        db.rollback()
        logger.error(f"DAG 업데이트 실패: {e}")
        raise


# @router.patch("/{dag_id}",
#               response_model=APIResponse[DAGResponse],
#               )
# @api_response_wrapper
# async def update_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
#     try:
#         dag_data = delete_dag_metadata(dag_id, db)
#         logger.info(f"Delete DAG metadata {dag_data}")
#         created_dag = create_dag_by_id(dag_id, dag, db)
#         db.commit()
#         logger.info(f"✅ Success to update DAG : {created_dag}")
#         return created_dag
#     except Exception as e:
#         db.rollback()
#         logger.error(f"DAG 업데이트 실패: {e}")
#         raise


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
    return [DAGResponse.from_dag(dag_version) for dag in db.query(Flow).all() for dag_version in dag.versions]


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


@router.get("/{dag_id}/dagRuns/{dag_run_id}/tasks")
@api_response_wrapper
async def get_tasks_of_dag_run(dag_id: str, dag_run_id: str,
                               airflow_client: AirflowClient = Depends(get_airflow_client),
                               flow: Flow = Depends(get_flow)):
    """
    get all tasks of DAG_runs
    :param dag_run_id:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    response = airflow_client.get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
    logger.info(f"airflow_client taskInstance response: {response}")
    task_mapper = {t.variable_id: t.id for t in flow.tasks}
    logger.info(f"task information for the current DAG: {task_mapper}")
    result = []
    for ti in response.get("task_instances", []):
        task_variable_id = ti['task_id']
        if task_variable_id in task_mapper:
            ti['task_id'] = task_mapper[task_variable_id]
            result.append(ti)
    return result


@router.get("/{dag_id}/dagRuns/{dag_run_id}/tasks/{task_id}")
@api_response_wrapper
async def get_task_of_dag_run(dag_id: str, dag_run_id: str, task_id: str,
                              airflow_client: AirflowClient = Depends(get_airflow_client),
                              flow: Flow = Depends(get_flow)):
    """
    get job history of DAG
    :param flow:
    :param task_id:
    :param dag_run_id:
    :param dag_id:
    :param airflow_client:
    :return:
    """
    task_variable_id = None
    for task in flow.tasks:
        if task.id == task_id:
            task_variable_id = task.variable_id
            break
    if task_variable_id is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    response = airflow_client.get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_variable_id}")
    logger.info(response)
    return response


@router.get("/{dag_id}/dagRuns/{dag_run_id}/result")
@api_response_wrapper
async def get_result_of_dag_run(dag_id: str, dag_run_id: str):
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
