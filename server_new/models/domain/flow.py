import hashlib
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import Any

from api.render_template import render_dag_script
from config import Config
from errors import WorkflowError
from utils.functions import make_flow_id_by_name

logger = logging.getLogger()


class Task:
    def __init__(self,
                 id_: str,
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
        self.write_time = datetime.now(timezone.utc)
        self._file_hash = None

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

    @property
    def file_hash(self):
        if self._file_hash is None:
            file_contents = self.write_file()
            self._file_hash = get_hash(file_contents)
        return self._file_hash

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


def get_hash(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def delete_dag_file(dag_id: str):
    dag_dir_path = os.path.join(Config.DAG_DIR, dag_id)
    dag_file_path = os.path.join(dag_dir_path, "dag.py")
    if os.path.exists(dag_file_path):
        os.remove(dag_file_path)
        logger.info(f"🗑 DAG 파일 삭제됨: {dag_file_path}")
    else:
        logger.warning(f"⚠️ DAG 파일이 존재하지 않음: {dag_file_path}")
    # 디렉토리 삭제 시도
    try:
        shutil.rmtree(dag_dir_path)
        logger.info(f"📂 DAG 디렉토리 삭제됨: {dag_dir_path}")
    except FileNotFoundError:
        logger.warning(f"⚠️ DAG 디렉토리가 존재하지 않음: {dag_dir_path}")
    except Exception as e:
        logger.error(f"❌ DAG 디렉토리 삭제 실패: {e}")
