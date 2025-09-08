import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from api.render_template import render_task_code_script, render_dag_script
from config import Config
from errors import WorkflowError
from repositories.system_function_repo import SystemFunctionRepo
from utils.functions import make_flow_id_by_name, get_hash, get_stable_hash

logger = logging.getLogger()


class SystemFunction:
    def __init__(self,
                 id_: str,
                 name: str,
                 description: str,
                 impl_namespace: str,
                 impl_callable: str,
                 python_libraries: list[str],
                 param_schema: list[dict[str, Any]],
                 is_deprecated: bool,
                 ):
        self.id = id_
        self.name = name
        self.description = description
        self.impl_namespace = impl_namespace
        self.impl_callable = impl_callable
        self.python_libraries = python_libraries
        self.param_schema = param_schema
        self.is_deprecated = is_deprecated


class Task:
    def __init__(self,
                 id_: str,
                 variable_id,
                 kind: str,
                 python_libraries,
                 code,
                 builtin_func_id,
                 ui_type,
                 ui_label,
                 ui_position,
                 ui_style,
                 input_properties: list[dict[str, Any]],
                 output_properties: list[dict[str, Any]],
                 inputs: dict[str, Any],
                 ui_class: str = None,
                 ui_extra_data=None,
                 ):
        self.id = id_
        self.variable_id = variable_id
        self.kind = kind
        self.python_libraries = python_libraries if kind == 'code' else None
        self.code = code
        self.builtin_func_id = builtin_func_id
        self.code_hash = get_hash(code) if kind == 'code' else None
        self.ui_class = ui_class
        self.ui_type = ui_type
        self.ui_label = ui_label
        self.ui_position = ui_position
        self.ui_style = ui_style
        self.input_properties = input_properties
        self.output_properties = output_properties
        self.inputs = inputs
        self._system_function = None
        self.ui_extra_data = ui_extra_data

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return get_stable_hash(
            self.variable_id,
            self.code_hash,
            json.dumps(self.python_libraries, sort_keys=True),
            self.ui_type,
            self.ui_label,
            json.dumps(self.ui_position, sort_keys=True),
            json.dumps(self.ui_style, sort_keys=True),
            json.dumps(self.inputs, sort_keys=True),
        )

    @property
    def system_function(self):
        if self._system_function is None:
            system_function_repo = SystemFunctionRepo()
            sf = system_function_repo.find_by_id(self.builtin_func_id)
            if sf:
                self._system_function = SystemFunction(
                    id_=sf.id,
                    name=sf.name,
                    description=sf.description,
                    impl_namespace=sf.impl_namespace,
                    impl_callable=sf.impl_callable,
                    python_libraries=sf.python_libraries,
                    param_schema=sf.param_schema,
                    is_deprecated=sf.is_deprecated,
                )
        return self._system_function


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
        return get_stable_hash(
            hash(self.source),
            hash(self.target),
            self.ui_type,
            self.ui_label,
            json.dumps(self.ui_label_style, sort_keys=True),
            json.dumps(self.ui_label_bg_style, sort_keys=True),
            json.dumps(self.ui_label_bg_padding, sort_keys=True),
            self.ui_label_bg_border_radius,
            json.dumps(self.ui_style, sort_keys=True),
        )


class Flow:
    def __init__(self,
                 name: str,
                 description: str,
                 owner_id: str,
                 scheduled: str,
                 schedule_options: dict[str, Any],
                 tasks: list[Task],
                 edges: list[Edge],
                 is_draft: bool,
                 max_retries: int,
                 _id: str = None,
                 updated_at: datetime | None = None,
                 active_status: bool = False,
                 execution_status: str | None = None,
                 is_deleted: bool = False,
                 ):
        self.id = _id if _id else str(uuid.uuid4())
        self.name = name
        self.dag_id = make_flow_id_by_name(name, is_draft)
        self.description = description
        self.owner_id = owner_id
        self.scheduled = scheduled
        self.schedule_options = schedule_options
        self.tasks = tasks
        self.edges = edges
        self.write_time = datetime.now(timezone.utc)
        self._file_hash = None
        self.updated_at = updated_at
        self.active_status = active_status
        self.execution_status = execution_status
        self.is_draft = is_draft
        self.max_retries = max_retries
        self.is_deleted = is_deleted

    def __eq__(self, other):
        if not isinstance(other, Flow):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        task_hashes = sorted(hash(t) for t in self.tasks)  # Task 순서 무시
        edge_hashes = sorted(hash(e) for e in self.edges)  # Edge 순서 무시
        return get_stable_hash(
            self.name,
            self.description,
            self.owner_id,
            self.scheduled,
            task_hashes,
            edge_hashes,
            self.active_status,
        )

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
            if task.kind == 'code':
                file_contents = render_task_code_script(
                    task_code=task.code,
                    kind=task.kind,
                    params=task.inputs,
                )
            else:
                default_inputs = {col.get('name'): col.get('value') for col in task.system_function.param_schema}
                default_inputs.update(task.inputs)
                default_inputs['is_test'] = self.is_draft
                file_contents = render_task_code_script(
                    task_code=task.code,
                    kind=task.kind,
                    impl_namespace=task.system_function.impl_namespace,
                    impl_callable=task.system_function.impl_callable,
                    params=default_inputs,
                )
            with open(os.path.join(dag_dir_path, f"func_{task.variable_id}.py"), 'w') as dag_file:
                dag_file.write(file_contents)

        dag_file_path = os.path.join(dag_dir_path, f"{self.dag_id}.py")
        try:
            # write dag
            file_contents = render_dag_script(self.dag_id,
                                              self.tasks,
                                              self.edges,
                                              tags=[self.dag_id, "draft" if self.is_draft else "publish", "generated"],
                                              schedule=self.scheduled if not self.is_draft else None,
                                              )
            with open(dag_file_path, 'w') as dag_file:
                dag_file.write(file_contents)
            return file_contents
        except Exception as e:
            if os.path.exists(dag_file_path):
                os.remove(dag_file_path)
                logger.warning(f"🧹 Delete file: {dag_file_path}")
            raise WorkflowError(f"❌ DAG 파일 생성 실패: {e}")
