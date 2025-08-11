import datetime

from sqlalchemy.orm import Session

from errors import WorkflowError
from models.api.dag_model import DAGRequest, DAGNode, DAGEdge, DAGResponse, DAGNodeData
from models.db.airflow_mapper import AirflowDag
from models.db.edge import Edge as DBEdge
from models.db.flow import Flow as DBFlow
from models.db.task import Task as DBTask, TaskInput
from models.domain.flow import Flow as DomainFlow, Edge as DomainEdge, Task as DomainTask
from utils.code_validator import validate_user_code


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
        if error_list := validate_user_code(task.data.code):
            errors[task.id] = error_list
        result[task.id] = DomainTask(task.id,
                                     f"task_{i}",
                                     task.data.kind.lower(),
                                     task.data.python_libraries,
                                     task.data.code,
                                     task.type,
                                     task.data.label,
                                     task.position,
                                     task.style,
                                     task.data.input_meta_type,
                                     task.data.output_meta_type,
                                     task.data.inputs,
                                     )
    if errors:
        raise WorkflowError(errors)
    return result


def flow_api2domain(dag: DAGRequest):
    tasks = task_api2domain(dag.nodes)
    return DomainFlow(
        name=dag.name,
        description=dag.description,
        owner=dag.owner,
        scheduled=dag.schedule,
        tasks=list(tasks.values()),
        edges=edge_api2domain(dag.edges, tasks),
        is_draft=dag.is_draft,
        max_retires=dag.max_retires,
    )


def flow_db2domain(flow: DBFlow):
    tasks_cache: dict[str, DomainTask] = {task.id: DomainTask(
        task.id,
        task.variable_id,
        task.kind,
        task.python_libraries,
        task.code_string,
        task.ui_type,
        task.ui_label,
        task.ui_position,
        task.ui_style,
        task.input_meta_type,
        task.output_meta_type,
        {inp.key: inp.value for inp in task.inputs},
        impl_namespace=task.impl_namespace,
        impl_callable=task.impl_callable,
    ) for task in flow.tasks}
    return DomainFlow(
        name=flow.name,
        description=flow.description,
        owner=flow.owner_id,
        scheduled=flow.schedule,
        tasks=list(tasks_cache.values()),
        edges=[DomainEdge(
            edge.id,
            tasks_cache[edge.from_task_id],
            tasks_cache[edge.to_task_id],
            edge.ui_type,
            edge.ui_label,
            edge.ui_labelStyle,
            edge.ui_labelBgStyle,
            edge.ui_labelBgPadding,
            edge.ui_labelBgBorderRadius,
            edge.ui_style,
        ) for edge in flow.edges],
        is_draft=flow.is_draft,
        max_retires=flow.max_retires,
        _id=flow.id,
        updated_at=flow.updated_at,
        active_status=flow.active_status,
        execution_status=flow.flow_execution_queues[0].status if flow.flow_execution_queues else None,
    )


def flow_domain2api(flow: DomainFlow):
    return DAGResponse(
        id=flow.id,
        name=flow.name,
        description=flow.description,
        owner=flow.owner,
        # TODO: task, edge 변환
        nodes=[DAGNode(
            id=task.id,
            type=task.ui_type,
            position=task.ui_position,
            data=DAGNodeData(
                label=task.ui_label,
                kind=task.kind,
                python_libraries=task.python_libraries,
                code=task.code,
                input_meta_type=task.input_meta_type,
                output_meta_type=task.output_meta_type,
                inputs=task.inputs,
            ),
            style=task.ui_style,
        ) for task in flow.tasks],
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
        updated_at=flow.updated_at,
        active_status=flow.active_status,
        execution_status=flow.execution_status,
        is_draft=flow.is_draft,
        max_retires=flow.max_retires,
    )


def task_edge_domain2db(flow: DBFlow, domain_edges: list[DomainEdge]):
    def task_domain2db(flow_: DBFlow, domain_task: DomainTask):
        task = DBTask(
            flow=flow_,
            variable_id=domain_task.variable_id,
            kind=domain_task.kind,
            input_meta_type=domain_task.input_meta_type,
            output_meta_type=domain_task.output_meta_type,
            ui_type=domain_task.ui_type,
            ui_label=domain_task.ui_label,
            ui_position=domain_task.ui_position,
            ui_style=domain_task.ui_style,
        )
        if domain_task.kind == "code":
            task.python_libraries = domain_task.python_libraries
            task.code_string = domain_task.code
            task.code_hash = domain_task.code_hash
        elif domain_task.kind == "meta":
            task.python_libraries = ["pandas", "requests"]
            task.impl_namespace = "builtin_functions"
            task.impl_callable = "run"
        else:  # system
            task.python_libraries = ["pandas", "requests"]
            task.impl_namespace = "builtin_functions"
            task.impl_callable = "run"
        task_inputs = []
        for k, v in domain_task.inputs.items():
            task_inputs.append(TaskInput(task=task, key=k, value=v))
        task.inputs = task_inputs
        return task

    tasks_cache: dict[str, DBTask] = {}
    edges: list[DBEdge] = []

    # 모든 edge 반복하며 task + edge 생성
    for domain_edge in domain_edges:
        source_key = domain_edge.source.id
        target_key = domain_edge.target.id

        if source_key not in tasks_cache:
            tasks_cache[source_key] = task_domain2db(flow, domain_edge.source)
        if target_key not in tasks_cache:
            tasks_cache[target_key] = task_domain2db(flow, domain_edge.target)

        source_task = tasks_cache[source_key]
        target_task = tasks_cache[target_key]

        db_edge = DBEdge(
            flow=flow,
            from_task=source_task,
            to_task=target_task,
            ui_type=domain_edge.ui_type,
            ui_label=domain_edge.ui_label,
            ui_labelStyle=domain_edge.ui_label_style,
            ui_labelBgPadding=domain_edge.ui_label_bg_padding,
            ui_labelBgBorderRadius=domain_edge.ui_label_bg_border_radius,
            ui_style=domain_edge.ui_style,
        )
        edges.append(db_edge)
    return list(tasks_cache.values()), edges


def flow_domain2db(domain_flow: DomainFlow, airflow_db: Session):
    flow = DBFlow(
        name=domain_flow.name,
        dag_id=domain_flow.dag_id,
        description=domain_flow.description,
        owner_id=domain_flow.owner,
        hash=hash(domain_flow),
        file_hash=domain_flow.file_hash,
        schedule=domain_flow.scheduled,
        max_retires=domain_flow.max_retires,
        is_loaded_by_airflow=check_loaded_by_airflow(domain_flow.write_time, domain_flow.dag_id, airflow_db),
    )
    # 관계 설정
    flow.tasks, flow.edges = task_edge_domain2db(flow, domain_flow.edges)
    return flow


def check_loaded_by_airflow(write_file_time: datetime.datetime, dag_id: str, airflow_db: Session):
    airflow_dag = airflow_db.query(AirflowDag).filter(AirflowDag.dag_id == dag_id).first()
    if airflow_dag is None:
        return False
    if airflow_dag.last_parsed_time:
        return airflow_dag.last_parsed_time > write_file_time
    else:
        return False
