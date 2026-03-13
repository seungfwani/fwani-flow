"""단위 테스트: get_snapshot_payload_hash 변화점 감지 (variable_id 기준 edges 포함)."""
import sys
import unittest
from pathlib import Path

# server_new 를 경로에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.snapshot import get_snapshot_payload_hash


def _minimal_payload(flow_id="f1", flow_name="Test", flow_hash=None):
    return {
        "flow": {
            "id": flow_id,
            "name": flow_name,
            "description": None,
            "owner_id": None,
            "hash": flow_hash or "0",
            "schedule": None,
            "is_deleted": False,
            "max_retries": 0,
        },
        "tasks": [],
        "edges": [],
    }


def _task(vid, tid=None):
    return {
        "id": tid or vid,
        "variable_id": vid,
        "kind": "code",
        "code_hash": "x",
        "python_libraries": [],
        "builtin_func_id": None,
        "input_properties": [],
        "output_properties": [],
        "ui_type": "default",
        "ui_label": "L",
        "ui_position": {},
        "ui_style": {},
        "ui_extra_data": {},
        "inputs": [],
    }


class TestSnapshotHash(unittest.TestCase):
    """get_snapshot_payload_hash 변화점 감지."""

    def test_same_payload_same_hash(self):
        """동일 payload면 해시 동일."""
        p = _minimal_payload()
        _, h1 = get_snapshot_payload_hash(p)
        _, h2 = get_snapshot_payload_hash(p)
        self.assertEqual(h1, h2)

    def test_different_flow_meta_different_hash(self):
        """flow 메타가 다르면 해시 다름."""
        _, h1 = get_snapshot_payload_hash(_minimal_payload(flow_name="A"))
        _, h2 = get_snapshot_payload_hash(_minimal_payload(flow_name="B"))
        self.assertNotEqual(h1, h2)

    def test_task_id_change_same_hash(self):
        """태스크 id만 바뀌고 variable_id 동일하면 해시 동일 (웹에서 id가 바뀌는 경우)."""
        base = {
            "flow": {"id": "f1", "name": "F", "description": None, "owner_id": None, "hash": "0", "schedule": None, "is_deleted": False, "max_retries": 0},
            "tasks": [_task("v1", "task-a"), _task("v2", "task-b")],
            "edges": [{"id": "e1", "from_task_id": "task-a", "to_task_id": "task-b", "ui_type": "default"}],
        }
        _, h1 = get_snapshot_payload_hash(base)
        base2 = {
            **base,
            "tasks": [_task("v1", "task-a-new"), _task("v2", "task-b-new")],
            "edges": [{"id": "e1-new", "from_task_id": "task-a-new", "to_task_id": "task-b-new", "ui_type": "default"}],
        }
        _, h2 = get_snapshot_payload_hash(base2)
        self.assertEqual(h1, h2)

    def test_edge_variable_id_change_different_hash(self):
        """연결 구조(from/to의 variable_id)가 바뀌면 해시 다름."""
        base = {
            "flow": {"id": "f1", "name": "F", "description": None, "owner_id": None, "hash": "0", "schedule": None, "is_deleted": False, "max_retries": 0},
            "tasks": [_task("v1", "t1"), _task("v2", "t2"), _task("v3", "t3")],
            "edges": [{"id": "e1", "from_task_id": "t1", "to_task_id": "t2", "ui_type": "default"}],
        }
        _, h1 = get_snapshot_payload_hash(base)
        base2 = {**base, "edges": [{"id": "e1", "from_task_id": "t1", "to_task_id": "t3", "ui_type": "default"}]}
        _, h2 = get_snapshot_payload_hash(base2)
        self.assertNotEqual(h1, h2)

    def test_edge_ui_only_same_hash(self):
        """엣지 ui_type/ui_label만 다르고 연결 구조가 같으면 해시 동일."""
        base = {
            "flow": {"id": "f1", "name": "F", "description": None, "owner_id": None, "hash": "0", "schedule": None, "is_deleted": False, "max_retries": 0},
            "tasks": [_task("v1", "t1"), _task("v2", "t2")],
            "edges": [{"id": "e1", "from_task_id": "t1", "to_task_id": "t2", "ui_type": "default"}],
        }
        _, h1 = get_snapshot_payload_hash(base)
        base2 = {**base, "edges": [{"id": "e1", "from_task_id": "t1", "to_task_id": "t2", "ui_type": "other", "ui_label": "x"}]}
        _, h2 = get_snapshot_payload_hash(base2)
        self.assertEqual(h1, h2)


if __name__ == "__main__":
    unittest.main()
