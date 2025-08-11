import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from api.render_template import render_dag_script, render_task_code_script
from config import Config
from errors import WorkflowError
from utils.functions import make_flow_id_by_name, get_hash

logger = logging.getLogger()


class Task:
    def __init__(self,
                 id_: str,
                 variable_id,
                 kind: str,
                 python_libraries,
                 code,
                 ui_type,
                 ui_label,
                 ui_position,
                 ui_style,
                 input_meta_type: dict[str, Any],
                 output_meta_type: dict[str, Any],
                 inputs: dict[str, Any],
                 impl_namespace=None,
                 impl_callable=None,
                 ):
        self.id = id_
        self.variable_id = variable_id
        self.kind = kind
        self.python_libraries = python_libraries
        self.code = code
        self.code_hash = get_hash(code) if kind == 'code' else None
        self.ui_type = ui_type
        self.ui_label = ui_label
        self.ui_position = ui_position
        self.ui_style = ui_style
        self.input_meta_type = input_meta_type
        self.output_meta_type = output_meta_type
        self.inputs = inputs
        self.impl_namespace = impl_namespace
        self.impl_callable = impl_callable

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
                 id_: str,
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
        self.id = id_
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
                 is_draft: bool,
                 max_retires: int,
                 _id: str = None,
                 updated_at: datetime | None = None,
                 active_status: bool = False,
                 execution_status: str | None = None,
                 ):
        self.id = _id if _id else str(uuid.uuid4())
        self.name = name
        self.dag_id = make_flow_id_by_name(name, is_draft)
        self.description = description
        self.owner = owner
        self.scheduled = scheduled
        self.tasks = tasks
        self.edges = edges
        self.write_time = datetime.now(timezone.utc)
        self._file_hash = None
        self.updated_at = updated_at
        self.active_status = active_status
        self.execution_status = execution_status
        self.is_draft = is_draft
        self.max_retires = max_retires

    def __eq__(self, other):
        if not isinstance(other, Flow):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((
            self.name,
            self.is_draft,
            self.description,
            self.owner,
            self.scheduled,
            tuple(self.tasks),
            tuple(self.edges),
            self.active_status,
        ))

    @property
    def file_hash(self):
        if self._file_hash is None:
            file_contents = self.write_file()
            self._file_hash = get_hash(file_contents)
        return self._file_hash

    def write_file(self):
        dag_dir_path = os.path.join(Config.DAG_DIR, self.dag_id)
        os.makedirs(dag_dir_path, exist_ok=True)
        # write dag
        for task in self.tasks:
            file_contents = render_task_code_script(
                task_code=task.code,
                kind=task.kind,
                impl_namespace=task.impl_namespace,
                impl_callable=task.impl_callable,
                params=task.inputs,
            )
            with open(os.path.join(dag_dir_path, f"func_{task.variable_id}.py"), 'w') as dag_file:
                dag_file.write(file_contents)

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
