from __future__ import annotations

import datetime

from sqlalchemy.orm import Session

from errors import WorkflowError
from models.api.dag_model import DAGRequest, DAGNode, DAGEdge, DAGResponse
from models.db.airflow_mapper import AirflowDag
from models.db.flow import Flow as DBFlow, FlowSnapshot
from models.domain.execution_status_presenter import get_execution_status_presenter
from models.domain.flow import Flow as DomainFlow, Edge as DomainEdge, Task as DomainTask


def edge_api2domain(edges: list[DAGEdge], tasks: dict[str, DomainTask]) -> list[DomainEdge]:
    return [DomainEdge(edge.id,
                       tasks[edge.source],
                       tasks[edge.target],
                       edge.type,
                       edge.label,
                       edge.labelStyle,
                       edge.labelBgStyle,
                       edge.labelBgPadding,
                       edge.labelBgBorderRadius,
                       edge.style,
                       ) for edge in edges
            if edge.source in tasks
            and edge.target in tasks
            ]


def task_api2domain(tasks: [DAGNode]) -> dict[str, DomainTask]:
    result = {}
    errors = {}
    for i, task in enumerate(tasks):
        if task.data.kind.lower() == 'meta' and not task.data.builtin_func_id:
            builtin_function_id = '00000000-0000-4000-9000-000000000001'
        else:
            builtin_function_id = task.data.builtin_func_id

        result[task.id] = DomainTask(task.id,
                                     f"task_{i}",
                                     task.data.kind.lower(),
                                     task.data.python_libraries,
                                     task.data.code,
                                     builtin_function_id,
                                     task.type,
                                     task.data.label,
                                     task.position,
                                     task.style,
                                     task.data.input_properties,
                                     task.data.output_properties,
                                     task.data.inputs,
                                     ui_class=task.class_,
                                     ui_extra_data=task.data.to_json()
                                     )
    if errors:
        raise WorkflowError(errors)
    return result


def flow_api2domain(dag: DAGRequest, dag_id: str = None):
    tasks = task_api2domain(dag.nodes)
    return DomainFlow(
        _id=dag_id,
        name=dag.name,
        description=dag.description,
        owner_id=dag.owner_id,
        scheduled=dag.schedule,
        schedule_options=dag.schedule_options,
        tasks=list(tasks.values()),
        edges=edge_api2domain(dag.edges, tasks),
        is_draft=dag.is_draft,
        max_retries=dag.max_retries,
    )


def flow_domain2api(flow: DomainFlow):
    if not flow:
        return None
    tasks = []
    for task in flow.tasks:
        if task.ui_extra_data:
            data = task.ui_extra_data
            data['label'] = task.ui_label
            data['kind'] = task.kind
            data['python_libraries'] = task.python_libraries
            data['code'] = task.code
            data['builtin_func_id'] = task.builtin_func_id
            data['inputs'] = task.inputs
        else:
            data = {
                "label": task.ui_label,
                "kind": task.kind,
                "python_libraries": task.python_libraries,
                "code": task.code,
                "builtin_func_id": task.builtin_func_id or "",
                "inputs": task.inputs,
            }
        tasks.append(DAGNode(
            id=task.id,
            type=task.ui_type,
            position=task.ui_position,
            data=data,
            style=task.ui_style,
            class_=task.ui_class,
        ))
    return DAGResponse(
        id=flow.id,
        name=flow.name,
        description=flow.description,
        owner_id=flow.owner_id,
        # TODO: task, edge 변환
        nodes=tasks,
        edges=[DAGEdge(
            id=edge.id,
            type=edge.ui_type,
            source=edge.source.id,
            target=edge.target.id,
            label=edge.ui_label,
            labelStyle=edge.ui_label_style,
            labelBgStyle=edge.ui_label_bg_style,
            labelBgPadding=edge.ui_label_bg_padding,
            labelBgBorderRadius=edge.ui_label_bg_border_radius,
            style=edge.ui_style,
        ) for edge in flow.edges],
        schedule=flow.scheduled,
        schedule_options=flow.schedule_options,
        updated_at=flow.updated_at,
        active_status=flow.active_status,
        execution_status=get_execution_status_presenter().present(flow.execution_status),
        is_draft=flow.is_draft,
        max_retries=flow.max_retries,
    )


def payload_to_domain_flow(payload: dict) -> DomainFlow:
    """Build DomainFlow from snapshot payload for DAG file write / file_hash."""
    f = payload["flow"]
    task_list = payload.get("tasks", [])
    edge_list = payload.get("edges", [])
    tasks_by_id: dict[str, DomainTask] = {}
    for t in task_list:
        inputs = {inp["key"]: inp["value"] for inp in t.get("inputs", [])}
        builtin = t.get("builtin_func_id") or t.get("system_function_id")
        dt = DomainTask(
            t["id"],
            t["variable_id"],
            t["kind"],
            t.get("python_libraries") or [],
            t.get("code_string") or "",
            builtin,
            t.get("ui_type", "default"),
            t.get("ui_label", ""),
            t.get("ui_position", {"x": 0, "y": 0}),
            t.get("ui_style") or {},
            t.get("input_properties") or [],
            t.get("output_properties") or [],
            inputs,
            ui_class=t.get("ui_class"),
            ui_extra_data=t.get("ui_extra_data"),
        )
        tasks_by_id[t["id"]] = dt
    edges = []
    for e in edge_list:
        sid, tid = e["from_task_id"], e["to_task_id"]
        if sid not in tasks_by_id or tid not in tasks_by_id:
            continue
        edges.append(DomainEdge(
            e["id"],
            tasks_by_id[sid],
            tasks_by_id[tid],
            e.get("ui_type", "default"),
            e.get("ui_label"),
            e.get("ui_label_style", {}),
            e.get("ui_label_bg_style", {}),
            e.get("ui_label_bg_padding", []),
            e.get("ui_label_bg_border_radius", 0),
            e.get("ui_style", {}),
        ))
    return DomainFlow(
        name=f["name"],
        description=f.get("description"),
        owner_id=f.get("owner_id"),
        scheduled=f.get("schedule"),
        schedule_options=f.get("schedule_options", {}),
        tasks=list(tasks_by_id.values()),
        edges=edges,
        is_draft=f.get("is_draft", False),
        max_retries=f.get("max_retries", 0),
        _id=f.get("id"),
        active_status=f.get("active_status", False),
        is_deleted=f.get("is_deleted", False),
    )


def flow_snapshot2api(flow_snapshot: FlowSnapshot):
    if not flow_snapshot:
        return None
    payload = flow_snapshot.payload
    tasks = []
    for task in payload["tasks"]:
        if task["ui_extra_data"]:
            data = task["ui_extra_data"]
            data['label'] = task["ui_label"]
            data['kind'] = task["kind"]
            data['python_libraries'] = task["python_libraries"]
            data['code'] = task["code_string"]
            data['builtin_func_id'] = task.get("builtin_func_id", "")
            data['inputs'] = {inp['key']: inp['value'] for inp in task["inputs"]}
        else:
            data = {
                "label": task["ui_label"],
                "kind": task["kind"],
                "python_libraries": task["python_libraries"],
                "code": task["code_string"],
                "builtin_func_id": task.get("builtin_func_id", ""),
                "inputs": {inp['key']: inp['value'] for inp in task["inputs"]}
            }
        tasks.append(DAGNode(
            id=task["id"],
            type=task["ui_type"],
            position=task["ui_position"],
            class_=task.get("ui_class"),
            data=data,
            style=task["ui_style"],
        ))
    f = payload['flow']
    return DAGResponse(
        id=f['id'],
        name=f['name'],
        description=f['description'],
        owner_id=f['owner_id'],
        # TODO: task, edge 변환
        nodes=tasks,
        edges=[DAGEdge(
            id=e["id"],
            type=e["ui_type"],
            source=e["from_task_id"],
            target=e["to_task_id"],
            label=e["ui_label"],
            labelStyle=e["ui_label_style"],
            labelBgStyle=e["ui_label_bg_style"],
            labelBgPadding=e["ui_label_bg_padding"],
            labelBgBorderRadius=e["ui_label_bg_border_radius"],
            style=e["ui_style"],
        ) for e in payload["edges"]],
        schedule=f['schedule'],
        schedule_options=f.get("schedule_options"),
        updated_at=None,
        active_status=f['active_status'],
        execution_status=None,
        is_draft=f['is_draft'],
        max_retries=f['max_retries'],
    )


def flow_to_dag_list_response(db_flow: DBFlow, execution_status: str | None) -> DAGResponse:
    """목록용: 메타는 flow 행 기준, 그래프는 비움. 상세는 flow_snapshot2api 사용."""
    return DAGResponse(
        id=db_flow.id,
        name=db_flow.name,
        description=db_flow.description,
        owner_id=db_flow.owner_id,
        nodes=[],
        edges=[],
        schedule=db_flow.schedule,
        schedule_options=db_flow.schedule_options or {},
        is_draft=db_flow.is_draft,
        max_retries=db_flow.max_retries or 0,
        updated_at=db_flow.updated_at,
        active_status=db_flow.active_status,
        execution_status=get_execution_status_presenter().present(execution_status),
    )


def check_loaded_by_airflow(write_file_time: datetime.datetime, dag_id: str, airflow_db: Session):
    airflow_dag = airflow_db.query(AirflowDag).filter(AirflowDag.dag_id == dag_id).first()
    if airflow_dag is None:
        return False
    if airflow_dag.last_parsed_time:
        return airflow_dag.last_parsed_time > write_file_time
    else:
        return False
