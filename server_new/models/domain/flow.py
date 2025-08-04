import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from api.render_template import render_dag_script
from config import Config
from errors import WorkflowError
from models.api.dag_model import DAGNode, DAGEdge, DAGRequest, DAGResponse
from models.db.flow import Flow as DBFlow
from utils.functions import make_flow_id_by_name

logger = logging.getLogger()


class Task:
    def __init__(self,
                 id_,
                 variable_id,
                 python_libraries,
                 code,
                 ui_type,
                 ui_label,
                 ui_position,
                 ui_style,
                 input_meta_type: dict[str, Any],
                 output_meta_type: dict[str, Any],
                 inputs: dict[str, Any],
                 ):
        self.id = id_
        self.variable_id = variable_id
        self.python_libraries = python_libraries
        self.code = code
        self.code_hash = get_hash(code)
        self.ui_type = ui_type
        self.ui_label = ui_label
        self.ui_position = ui_position
        self.ui_style = ui_style
        self.input_meta_type = input_meta_type
        self.output_meta_type = output_meta_type
        self.inputs = inputs

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((
            self.id,
            self.variable_id,
            tuple(self.python_libraries),
            self.code_hash,
            self.ui_type,
            self.ui_label,
            tuple(self.ui_position),
            tuple(self.ui_style),
            tuple(self.input_meta_type),
            tuple(self.output_meta_type),
            tuple(self.inputs),
        ))


class Edge:
    def __init__(self,
                 source: Task,
                 target: Task,
                 ui_type,
                 ui_label,
                 ui_label_style,
                 ui_label_bg_style,
                 ui_label_bg_padding,
                 ui_label_bg_border_radius,
                 ui_style,
                 ):
        self.source = source
        self.target = target
        self.ui_type = ui_type
        self.ui_label = ui_label
        self.ui_label_style = ui_label_style
        self.ui_label_bg_style = ui_label_bg_style
        self.ui_label_bg_padding = ui_label_bg_padding
        self.ui_label_bg_border_radius = ui_label_bg_border_radius
        self.ui_style = ui_style

    def __eq__(self, other):
        if not isinstance(other, Edge):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((
            hash(self.source),
            hash(self.target),
            self.ui_type,
            self.ui_label,
            tuple(self.ui_label_style),
            tuple(self.ui_label_bg_style),
            tuple(self.ui_label_bg_padding),
            self.ui_label_bg_border_radius,
            tuple(self.ui_style),
        ))


class Flow:
    def __init__(self,
                 name: str,
                 description: str,
                 owner: str,
                 scheduled: str,
                 tasks: list[Task],
                 edges: list[Edge],
                 _id: str = None,
                 ):
        self.id = _id if _id else str(uuid.uuid4())
        self.name = name
        self.dag_id = make_flow_id_by_name(name)
        self.description = description
        self.owner = owner
        self.scheduled = scheduled
        self.tasks = tasks
        self.edges = edges
        file_contents = self.write_file()
        self.write_time = datetime.now(timezone.utc)
        self.file_hash = get_hash(file_contents)

    def __eq__(self, other):
        if not isinstance(other, Flow):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((
            self.name,
            self.description,
            self.owner,
            self.scheduled,
            tuple(self.tasks),
            tuple(self.edges),
        ))

    def to_dag_response(self):
        return DAGResponse(
            id = self.id,
            name=self.name,
            description=self.description,
            owner =self.owner,
            scheduled=self.scheduled,
            # TODO: task, edge 변환
            nodes=[self.tasks],
            edges=[self.edges],
        )

    @staticmethod
    def from_dag_request(dag: DAGRequest):
        tasks = convert_tasks(dag.nodes)
        return Flow(
            dag.name,
            dag.description,
            dag.owner,
            dag.schedule,
            tasks,
            convert_edges(dag.edges, tasks),
        )

    @staticmethod
    def from_db_flow(flow: DBFlow):
        tasks_cache: dict[str, Task] = {task.id: Task(
            task.id,
            task.variable_id,
            task.python_libraries,
            task.code_string,
            task.ui_type,
            task.ui_label,
            task.ui_position,
            task.ui_style,
            task.input_meta_type,
            task.output_meta_type,
            {inp.key: inp.value for inp in task.inputs},
        ) for task in flow.tasks}
        return Flow(
            flow.name,
            flow.description,
            flow.owner_id,
            flow.schedule,
            list(tasks_cache.values()),
            [Edge(
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
            flow.id
        )

    def write_file(self):
        dag_dir_path = os.path.join(Config.DAG_DIR, self.dag_id)
        os.makedirs(dag_dir_path, exist_ok=True)
        dag_file_path = os.path.join(dag_dir_path, "dag.py")
        try:
            # write dag
            file_contents = render_dag_script(self.dag_id,
                                              self.tasks,
                                              self.edges,
                                              tags=[self.dag_id, "generated"],
                                              schedule=self.scheduled)
            with open(dag_file_path, 'w') as dag_file:
                dag_file.write(file_contents)
            return file_contents
        except Exception as e:
            msg = f"❌ DAG 파일 생성 실패: {e}"
            logger.error(msg)
            if os.path.exists(dag_file_path):
                os.remove(dag_file_path)
                logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
            raise WorkflowError(msg)


def convert_tasks(tasks: [DAGNode]) -> list[Task]:
    return [Task(task.id,
                 f"task_{i}",
                 task.data.python_libraries,
                 task.data.code,
                 task.type,
                 task.data.label,
                 task.position,
                 task.style,
                 task.data.input_meta_type,
                 task.data.output_meta_type,
                 task.data.inputs,
                 ) for i, task in enumerate(tasks)]


def convert_edges(edges: list[DAGEdge], tasks: list[Task]) -> list[Edge]:
    tasks_dict = {t.id: t for t in tasks}
    return [Edge(tasks_dict[edge.source],
                 tasks_dict[edge.target],
                 edge.type,
                 edge.label,
                 edge.labelStyle,
                 edge.labelBgStyle,
                 edge.labelBgPadding,
                 edge.labelBgBorderRadius,
                 edge.style,
                 ) for edge in edges if edge.source in tasks_dict and edge.target in tasks_dict]


def get_hash(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()
