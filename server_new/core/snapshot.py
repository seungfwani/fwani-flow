import json
from enum import Enum

from models.db.flow import Flow as DBFlow
from models.domain.flow import Flow as DomainFlow
from utils.functions import get_hash


class SnapshotOperation(Enum):
    DUMMY = "dummy"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"
    PUBLISH = "publish"


def get_snapshot_payload_hash(payload: dict) -> tuple[dict, str]:
    flow = payload["flow"]
    normalized_payload = {
        "flow": {
            "name": flow["name"],
            "description": flow["description"],
            "owner_id": flow["owner_id"],
            "hash": str(flow["hash"]),
            "schedule": flow["schedule"],
            "is_deleted": flow["is_deleted"],
            "max_retries": flow["max_retries"],
        },
        "tasks": [
            {
                "variable_id": t["variable_id"],
                "kind": t["kind"],
                "code_hash": t["code_hash"],
                "python_libraries": t["python_libraries"],
                "builtin_func_id": t["builtin_func_id"],
                "input_properties": t["input_properties"],
                "output_properties": t["output_properties"],
                "ui_type": t["ui_type"],
                "ui_label": t["ui_label"],
                "ui_position": t["ui_position"],
                "ui_style": t["ui_style"],
                "ui_extra_data": t["ui_extra_data"],
                "inputs": [
                    {
                        "key": inp["key"],
                        "type": inp["type"],
                        "value": inp["value"],
                    }
                    for inp in sorted(t["inputs"], key=lambda x: x["key"])
                ]
            }
            for t in sorted(payload["tasks"], key=lambda x: x["variable_id"])
        ],
    }
    return normalized_payload, get_hash(json.dumps(normalized_payload))


def build_flow_snapshot(flow: DBFlow) -> dict:
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
            "schedule": flow.schedule,
            "schedule_options": flow.schedule_options,
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
                "input_properties": t.input_properties,
                "output_properties": t.output_properties,
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
                "ui_label_style": e.ui_labelStyle,
                "ui_label_bg_style": e.ui_labelBgStyle,
                "ui_label_bg_padding": e.ui_labelBgPadding,
                "ui_label_bg_border_radius": e.ui_labelBgBorderRadius,
                "ui_style": e.ui_style,
            } for e in flow.edges
        ]
    }

    return snapshot


def build_flow_snapshot_by_domain(new_flow: DomainFlow, dag_id: str) -> dict:
    snapshot = {
        "flow": {
            "id": dag_id,
            "name": new_flow.name,
            "is_draft": new_flow.is_draft,
            "dag_id": new_flow.dag_id,
            "description": new_flow.description,
            "owner_id": new_flow.owner_id,
            "hash": str(hash(new_flow)),
            "file_hash": new_flow.file_hash,
            "schedule": new_flow.scheduled,
            "schedule_options": new_flow.schedule_options,
            "is_deleted": new_flow.is_deleted,
            "active_status": new_flow.active_status,
            "max_retries": new_flow.max_retries,
        },
        "tasks": [
            {
                "id": t.id,
                "variable_id": t.variable_id,
                "kind": t.kind,
                "code_string": t.code,
                "code_hash": t.code_hash,
                "python_libraries": t.python_libraries,
                "builtin_func_id": t.builtin_func_id,
                "input_properties": t.input_properties,
                "output_properties": t.output_properties,
                "ui_type": t.ui_type,
                "ui_label": t.ui_label,
                "ui_position": t.ui_position,
                "ui_class": t.ui_class,
                "ui_style": t.ui_style,
                "ui_extra_data": t.ui_extra_data,
                "inputs": [
                    {
                        "key": k,
                        "type": "string",
                        "value": v,
                    }
                    for k, v in t.inputs.items()
                ]
            } for t in new_flow.tasks
        ],
        "edges": [
            {
                "id": e.id,
                "from_task_id": e.source.id,
                "to_task_id": e.target.id,
                "ui_type": e.ui_type,
                "ui_label": e.ui_label,
                "ui_label_style": e.ui_label_style,
                "ui_label_bg_style": e.ui_label_bg_style,
                "ui_label_bg_padding": e.ui_label_bg_padding,
                "ui_label_bg_border_radius": e.ui_label_bg_border_radius,
                "ui_style": e.ui_style,
            } for e in new_flow.edges
        ]
    }

    return snapshot
