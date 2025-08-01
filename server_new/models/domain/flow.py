import hashlib
import logging
import os
from datetime import datetime
from typing import List, Any

from api.render_template import render_dag_script
from config import Config
from errors import WorkflowError
from models.api.dag_model import DAGNode, DAGEdge
from utils.functions import make_flow_id_by_name

logger = logging.getLogger()


class Task:
    def __init__(self,
                 id,
                 index,
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
        self.id = id
        self.variable_id = f"task_{index}"
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
                 ui_label_bg_borderRadius,
                 ui_style,
                 ):
        self.source = source
        self.target = target
        self.ui_type = ui_type
        self.ui_label = ui_label
        self.ui_label_style = ui_label_style
        self.ui_label_bg_style = ui_label_bg_style
        self.ui_label_bg_padding = ui_label_bg_padding
        self.ui_label_bg_borderRadius = ui_label_bg_borderRadius
        self.ui_style = ui_style

    def __eq__(self, other):
        if not isinstance(other, Edge):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((
            self.source,
            self.target,
            self.ui_type,
            self.ui_label,
            self.ui_label_style,
            self.ui_label_bg_style,
            self.ui_label_bg_padding,
            self.ui_label_bg_borderRadius,
            self.ui_style,
        ))


class Flow:
    def __init__(self,
                 name: str,
                 description: str,
                 owner: str,
                 scheduled: str,
                 tasks: list[DAGNode],
                 edges: list[DAGEdge],
                 ):
        self.id = make_flow_id_by_name(name)
        self.name = name
        self.description = description
        self.owner = owner
        self.scheduled = scheduled
        self.tasks = convert_tasks(tasks)
        self.edges = convert_edges(edges, tasks)
        file_contents = self.write_file()
        self.write_time = datetime.now()
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

    def write_file(self):
        dag_dir_path = os.path.join(Config.DAG_DIR, self.id)
        os.makedirs(dag_dir_path, exist_ok=True)
        dag_file_path = os.path.join(dag_dir_path, "dag.py")
        try:
            # write dag
            file_contents = render_dag_script(self.id,
                                              self.tasks,
                                              self.edges,
                                              tags=[self.id, "generated"],
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


def convert_tasks(tasks: [DAGNode]) -> List[Task]:
    return [Task(task.id,
                 i,
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
