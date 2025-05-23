import json
import json
import logging
import os
import traceback
import uuid
from typing import List, Optional

from fastapi import Depends, HTTPException
from sqlalchemy import desc, func, case
from sqlalchemy.orm import Session, joinedload, aliased

from api.models.dag_model import DAGRequest
from api.render_template import render_dag_script
from config import Config
from core.database import get_db
from models.airflow_dag_run_history import AirflowDagRunHistory
from models.edge import Edge
from models.edge_ui import EdgeUI
from models.flow import Flow
from models.flow_trigger_queue import FlowTriggerQueue
from models.flow_version import FlowVersion
from models.function_library import FunctionLibrary
from models.task import Task
from models.task_input import TaskInput
from models.task_ui import TaskUI
from utils.functions import make_flow_id_by_name, normalize_dag, calculate_dag_hash, normalize_task, get_airflow_dag_id, \
    get_hash
from utils.udf_validator import get_validated_inputs

logger = logging.getLogger()


def get_flows(db: Session):
    # 버전별 우선순위: draft가 있으면 draft, 없으면 최신 published
    version_rank = func.row_number().over(
        partition_by=FlowVersion.flow_id,
        order_by=(
            # draft 우선, 없으면 최신 updated_at
            case((FlowVersion.is_draft == True, 0), else_=1),
            FlowVersion.updated_at.desc(),
        )
    ).label("version_rank")

    # FlowVersion + rank
    ranked_subquery = (
        db.query(FlowVersion, version_rank)
        .subquery()
    )

    FlowVersionAlias = aliased(FlowVersion, ranked_subquery)

    # version_rank = 1 인 것만 선택
    flows = (
        db.query(FlowVersionAlias)
        .filter(ranked_subquery.c.version_rank == 1)
        .order_by(FlowVersionAlias.updated_at.desc())
        .all()
    )

    return flows


def get_versions_of_flow(flow_id: str, db: Session) -> List[FlowVersion]:
    return (
        db.query(FlowVersion)
        .filter(FlowVersion.flow_id == flow_id)
        .order_by(desc(FlowVersion.version))
        .all()
    )


def get_flow(flow_id: str, db: Session = Depends(get_db)):
    return db.query(Flow).filter(Flow.id == flow_id).first()


def get_all_flow_versions(flow_id: str, db: Session) -> List[FlowVersion]:
    return (db.query(FlowVersion).filter(FlowVersion.flow_id == flow_id)
            .order_by(desc(FlowVersion.version))
            .all())


def get_flow_version(db: Session, flow_id: str, version: int = 0, is_draft=False, eager_load=False) -> FlowVersion:
    if eager_load:
        query = db.query(FlowVersion).options(
            joinedload(FlowVersion.flow),
            joinedload(FlowVersion.tasks),
            joinedload(FlowVersion.edges),
        )
    else:
        query = db.query(FlowVersion)
    if is_draft:
        return query.filter_by(flow_id=flow_id, is_draft=True).first()
    else:
        return query.filter_by(flow_id=flow_id, version=version).first()


def get_flow_last_version(flow_id: str, db: Session) -> Optional[FlowVersion]:
    return (
        db.query(FlowVersion).filter(FlowVersion.flow_id == flow_id, FlowVersion.is_draft == False)
        .order_by(desc(FlowVersion.version))
        .first()
    )


def get_flow_last_version_or_draft(flow_id: str, db: Session):
    draft_version = get_flow_version(db, flow_id, is_draft=True)
    if draft_version:
        return draft_version

    last_flow_version = get_flow_last_version(flow_id, db)
    if not last_flow_version:
        raise ValueError(f"No versions found for flow {flow_id}")
    return last_flow_version


def delete_flow(flow_id: str, db: Session) -> Flow:
    logger.info(f"⚠️ Get DAG {flow_id} metadata for deleting")
    flow = (db.query(Flow).filter(Flow.id == flow_id)
            .options(joinedload(Flow.versions))
            .first())
    if not flow:
        raise ValueError(f"DAG {flow_id} not found")

    db.delete(flow)
    logger.info(f"🗑️ Delete DAG {flow_id} metadata")
    db.commit()
    return flow


def delete_flow_version(flow_id: str, db: Session, version: int = 0, is_draft=False):
    try:
        flow_version = get_flow_version(db, flow_id, version=version, is_draft=is_draft, eager_load=True)
        if not flow_version:
            raise ValueError(f"DAG {flow_id} version {version} draft {is_draft} not found")
        db.delete(flow_version)
        logger.info(f"⚠️ Delete DAG {flow_id} version {flow_version.version} draft {is_draft}")
        dag_file_path = os.path.join(Config.DAG_DIR, flow_id, "draft.py" if is_draft else f"v{version}.py")
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            logger.info(f"🗑️ Delete DAG file {dag_file_path}")
        db.commit()
        return flow_version
    except Exception as e:
        logger.error(f"Failed to delete DAG {flow_id} version {version} draft {is_draft} due to {e}", e)
        db.rollback()


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
    dag_id = make_flow_id_by_name(dag.name)
    return Flow(
        id=dag_id,
        name=dag.name,
        description=dag.description,
        owner=dag.owner,
    )


def is_flow_changed(new_dag: DAGRequest, latest_version_id: str, db: Session) -> bool:
    old_nodes = db.query(Task).filter(Task.flow_version_id == latest_version_id).all()
    old_edges = db.query(Edge).filter(Edge.flow_version_id == latest_version_id).all()

    normalized_new_dag = normalize_dag(new_dag)

    # 이미 존재하면 id 도 같아야함. 새로 추가된 id 는 다를 수 밖에 없음 (old 에는 존재하지 않으니까)
    old_node_set = [normalize_task(n) for n in old_nodes]
    new_node_set = normalized_new_dag.get("nodes")

    if old_node_set != new_node_set:
        return True

    return len(old_edges) != normalized_new_dag.get("edge_count")


def is_flow_changed_by_hash(old_hash: str, new_dag: DAGRequest) -> bool:
    new_hash = calculate_dag_hash(new_dag)
    return old_hash != new_hash


def write_dag_file(flow_version: FlowVersion):
    dag_dir_path = os.path.join(Config.DAG_DIR, flow_version.flow_id)
    os.makedirs(dag_dir_path, exist_ok=True)
    dag_version = "draft" if flow_version.is_draft else f"v{flow_version.version}"
    dag_file_path = os.path.join(dag_dir_path, dag_version)
    try:
        # write dag
        file_contents = render_dag_script(f"{flow_version.flow_id}__{dag_version}",
                                          flow_version.tasks,
                                          flow_version.edges,
                                          tags=[flow_version.flow_id, dag_version])
        with open(dag_file_path + ".py", 'w') as dag_file:
            dag_file.write(file_contents)
        flow_version.file_hash = get_hash(file_contents)
        flow_version.is_loaded_by_airflow = False
    except Exception as e:
        logger.error(f"❌ DAG 파일 생성 실패: {e}")
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


def create_draft_version(dag: DAGRequest, flow: Flow, db: Session, version=1) -> FlowVersion:
    # Create FlowVersion as draft
    version = FlowVersion(id=str(uuid.uuid4()),
                          flow_id=flow.id,
                          is_draft=True,
                          version=version,
                          hash=calculate_dag_hash(dag),
                          flow=flow)

    tasks = create_tasks(dag, db)
    edges = create_edges(dag, tasks)

    version.set_tasks(list(tasks.values()))
    version.set_edges(edges)
    db.add(version)
    return version


def update_draft_version(version: FlowVersion, dag: DAGRequest, db: Session) -> FlowVersion:
    # 1. 해당 draft의 task, edge 모두 삭제
    db.query(Task).filter(Task.flow_version_id == version.id).delete()
    db.query(Edge).filter(Edge.flow_version_id == version.id).delete()

    # 2. 버전 메타 정보도 갱신 (예: hash, updated_at)
    version.hash = calculate_dag_hash(dag)

    # 2. 새로운 node, edge 삽입
    tasks = create_tasks(dag, db)
    edges = create_edges(dag, tasks)

    version.set_tasks(list(tasks.values()))
    version.set_edges(edges)
    return version


def create_update_draft_dag(dag: DAGRequest, db: Session) -> FlowVersion:
    flow_id = make_flow_id_by_name(dag.name)
    try:
        existing_draft = get_flow_version(db, flow_id, is_draft=True)
        if existing_draft:  # draft O
            if not is_flow_changed(dag, existing_draft.id, db):  # 변경 X -> 기존 draft
                logger.info("⚠️ No draft version change")
                return existing_draft
            else:  # 변경 O -> update draft
                logger.info("🔄 Draft version changed")
                new_draft = update_draft_version(existing_draft, dag, db)
                write_dag_file(new_draft)
                db.commit()
                return new_draft
        else:  # draft X
            last_fv = get_flow_last_version(flow_id, db)  # check last publish
            if last_fv and not is_flow_changed_by_hash(last_fv.hash, dag):  # last O, 변경 X
                return last_fv
            flow = last_fv.flow if last_fv else create_flow(dag)
            next_version = (last_fv.version + 1) if last_fv else 1
            logger.info(f"🔄 Creating new draft version of {dag.name} ...")
            flow_version = create_draft_version(dag, flow, db, next_version)
            write_dag_file(flow_version)
            db.commit()
            return flow_version

    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        logger.error(traceback.format_exc())
        db.rollback()
        logger.warning(f"🔄 메타데이터 롤백")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


def publish_flow_version(flow_id: str, dag: DAGRequest, db: Session) -> FlowVersion:
    # 1. draft version 찾기
    draft = db.query(FlowVersion).filter_by(flow_id=flow_id, is_draft=True).first()
    if not draft:
        flow = get_flow(flow_id, db)
        if not flow:
            flow = create_flow(dag)
            draft = create_draft_version(dag, flow, db)
        else:
            if len(flow.versions) == 0:
                draft = create_draft_version(dag, flow, db, 1)
            else:
                last_flow_version = sorted(flow.versions, key=lambda v: v.version, reverse=True)[0]
                if not is_flow_changed(dag, last_flow_version.id, db):
                    logger.warning("✅ DAG 내용이 변경되지 않아 publish 생략")
                    return last_flow_version
                draft = create_draft_version(dag, flow, db, last_flow_version.version + 1)

    # 2. draft → publish 전환
    draft.is_draft = False
    db.flush()

    write_dag_file(draft)
    db.commit()
    return draft


def register_trigger(flow_version: FlowVersion, db: Session):
    trigger_entry = FlowTriggerQueue(
        id=str(uuid.uuid4()),
        flow_version_id=flow_version.id,
        dag_id=get_airflow_dag_id(flow_version),
        status="waiting",
        file_hash=flow_version.file_hash,
    )
    db.add(trigger_entry)
    db.commit()
    logger.info(f"🕒 DAG Trigger 등록 완료: {trigger_entry.dag_id}")
    return trigger_entry


def register_trigger_last_version_or_draft(flow_id: str, dag: Optional[DAGRequest], db: Session) -> FlowTriggerQueue:
    """
    DAG 파일이 작성된 후 실제 Airflow에 등록되었는지 확인하고,
    준비되면 dagRuns API를 호출하여 실행하도록 큐에 등록합니다.
    dag 가 None 이면, 마지막 퍼블리시 버전으로 트리거링
    dag 가 있으면, draft 와 비교후 트리거링
    """
    if not dag:
        flow_version = get_flow_last_version_or_draft(flow_id, db)
    else:
        flow_version = create_update_draft_dag(dag, db)
    if not flow_version:
        raise ValueError(f"No flow {flow_id} exists")
    return register_trigger(flow_version, db)


def register_trigger_specific_version(flow_id: str, version: int, db: Session) -> FlowTriggerQueue:
    flow_version = get_flow_version(db, flow_id, version)
    return register_trigger(flow_version, db)


def get_flow_run(flow_id: str, run_id: str, db: Session):
    return (
        db.query(FlowTriggerQueue)
        .filter(FlowTriggerQueue.dag_id.like(f"{flow_id}%"),
                FlowTriggerQueue.run_id == run_id)
        .first()
    )


def get_flow_runs(flow_id: str, db: Session):
    return (
        db.query(FlowTriggerQueue)
        .filter(FlowTriggerQueue.dag_id.like(f"{flow_id}%"))
        .all()
    )


def get_all_dag_runs_of_all_versions(flow_id: str, db: Session) -> [AirflowDagRunHistory]:
    flow_versions = get_all_flow_versions(flow_id, db)
    if not flow_versions:
        return []
    flow_version_ids = [fv.id for fv in flow_versions]
    return (
        db.query(AirflowDagRunHistory).filter(
            AirflowDagRunHistory.flow_version_id.in_(flow_version_ids)
        )
        .order_by(desc(AirflowDagRunHistory.created_at), desc(AirflowDagRunHistory.run_id))
        .all()
    )
