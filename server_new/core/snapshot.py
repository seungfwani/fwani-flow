import json
from enum import Enum

from models.db.flow import Flow
from utils.functions import get_hash


class SnapshotOperation(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"
    PUBLISH = "publish"


def _normalize_flow(flow: Flow) -> dict:
    # task 리스트를 variable_id 기준으로 정렬
    tasks_sorted = sorted(flow.tasks, key=lambda t: t.variable_id)

    # task별 inputs은 key 기준 정렬, id/비본질 필드 제외
    normal_tasks = []
    for t in tasks_sorted:
        normal_tasks.append({
            "variable_id": t.variable_id,
            "kind": t.kind,
            "code_string": t.code_string,
            "code_hash": t.code_hash,
            "python_libraries": t.python_libraries,
            "builtin_func_id": t.system_function_id,
            "input_meta_type": t.input_meta_type,
            "output_meta_type": t.output_meta_type,
            "ui_type": t.ui_type,
            "ui_label": t.ui_label,
            "ui_class": t.ui_class,
            "ui_position": t.ui_position,
            "ui_style": t.ui_style,
            "ui_extra_data": t.ui_extra_data,
            "inputs": [
                {
                    "key": inp.key,
                    "type": inp.type,
                    "value": inp.value,
                }
                for inp in sorted(t.inputs, key=lambda i: i.key)
            ],
        })

    # 플로우 본문에서 변동성 큰 값/파생값 제외하고 핵심만
    normal_flow = {
        "name": flow.name,
        "dag_id": flow.dag_id,
        "description": flow.description,
        "owner_id": flow.owner_id,
        "hash": flow.hash,
        "file_hash": flow.file_hash,
        "schedule": flow.schedule,
        "is_deleted": flow.is_deleted,
        "max_retries": flow.max_retries,
    }

    return {
        "flow": normal_flow,
        "tasks": normal_tasks,
    }


def build_flow_snapshot(flow: Flow) -> [dict, str]:
    snapshot = {
        "flow": {
            "id": flow.id,
            "name": flow.name,
            "is_draft": flow.is_draft,
            "dag_id": flow.dag_id,
            "description": flow.description,
            "owner_id": flow.owner_id,
            "hash": flow.hash,
            "file_hash": flow.file_hash,
            "is_loaded_by_airflow": flow.is_loaded_by_airflow,
            "schedule": flow.schedule,
            "is_deleted": flow.is_deleted,
            "active_status": flow.active_status,
            "max_retries": flow.max_retries,
        },
        "tasks": [
            {
                "id": t.id,
                "variable_id": t.variable_id,
                "kind": t.kind,
                "code_string": t.code_string,
                "code_hash": t.code_hash,
                "python_libraries": t.python_libraries,
                "builtin_func_id": t.system_function_id,
                "input_meta_type": t.input_meta_type,
                "output_meta_type": t.output_meta_type,
                "ui_type": t.ui_type,
                "ui_label": t.ui_label,
                "ui_position": t.ui_position,
                "ui_class": t.ui_class,
                "ui_style": t.ui_style,
                "ui_extra_data": t.ui_extra_data,
                "inputs": [
                    {
                        "id": inp.id,
                        "key": inp.key,
                        "type": inp.type,
                        "value": inp.value,
                    }
                    for inp in t.inputs
                ]
            } for t in flow.tasks
        ],
        "edges": [
            {
                "id": e.id,
                "from_task_id": e.from_task_id,
                "to_task_id": e.to_task_id,
                "ui_type": e.ui_type,
                "ui_label": e.ui_label,
                "ui_labelStyle": e.ui_labelStyle,
                "ui_labelBgStyle": e.ui_labelBgStyle,
                "ui_labelBgPadding": e.ui_labelBgPadding,
                "ui_labelBgBorderRadius": e.ui_labelBgBorderRadius,
                "ui_style": e.ui_style,
            } for e in flow.edges
        ]
    }

    # 2) 의미 기반(정규화) 스냅샷 → 해시 계산
    normalized = _normalize_flow(flow)
    return snapshot, get_hash(json.dumps(normalized))
