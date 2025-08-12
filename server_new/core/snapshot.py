from enum import Enum

from models.db.flow import Flow


class SnapshotOperation(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"
    PUBLISH = "publish"


def build_flow_snapshot(flow: Flow) -> dict:
    return {
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
                "impl_namespace": t.impl_namespace,
                "impl_callable": t.impl_callable,
                "input_meta_type": t.input_meta_type,
                "output_meta_type": t.output_meta_type,
                "ui_type": t.ui_type,
                "ui_label": t.ui_label,
                "ui_position": t.ui_position,
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
