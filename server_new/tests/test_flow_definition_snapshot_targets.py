"""FlowDefinitionService: 공개본(PUBLISHED) 저장 시 드래프트 스냅샷이 덮어쓰이지 않는지 검증."""
import copy
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_fd, _SQLITE_PATH = tempfile.mkstemp(suffix="_snapshot_targets.db")
os.close(_fd)
os.environ["DB_TYPE"] = "sqlite"
os.environ["DB_NAME"] = _SQLITE_PATH

from core.database import SessionLocalBaseDB
from core.services.flow_definition_service import FlowDefinitionService
from core.snapshot import SnapshotOperation, SnapshotTarget, get_snapshot_payload_hash
from models.db.flow import Flow as DBFlow, FlowSnapshot
from models.db.flow_snapshot_role import FlowSnapshotRole


def _base_payload(flow_id: str, name: str, description: str, *, active_status: bool = False):
    return {
        "flow": {
            "id": flow_id,
            "name": name,
            "is_draft": False,
            "dag_id": f"d_{name}",
            "description": description,
            "owner_id": None,
            "hash": "0",
            "file_hash": None,
            "schedule": None,
            "schedule_options": None,
            "is_deleted": False,
            "active_status": active_status,
            "max_retries": 0,
        },
        "tasks": [],
        "edges": [],
    }


class TestFlowDefinitionSnapshotTargets(unittest.TestCase):
    """snapshot_target=PUBLISHED 일 때 드래프트 행이 유지되는지."""

    def setUp(self):
        self.session = SessionLocalBaseDB()
        engine = self.session.get_bind()
        FlowSnapshot.__table__.drop(bind=engine, checkfirst=True)
        DBFlow.__table__.drop(bind=engine, checkfirst=True)
        DBFlow.__table__.create(bind=engine, checkfirst=True)
        FlowSnapshot.__table__.create(bind=engine, checkfirst=True)

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test_published_active_status_does_not_replace_draft_payload(self):
        """Flow.is_draft=True 이어도 공개본만 갱신하면 드래프트 페이로드는 유지된다."""
        class _Airflow:
            def update_pause(self, dag_id, paused):
                return paused

        dag_service = FlowDefinitionService(self.session, airflow_client=_Airflow())
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="TestFlow",
            dag_id="test_flow",
            is_draft=True,
            active_status=False,
        )
        self.session.add(flow)
        self.session.flush()

        pub = copy.deepcopy(_base_payload(flow_id, "TestFlow", "published-body", active_status=False))
        drf = copy.deepcopy(_base_payload(flow_id, "TestFlow", "draft-only-body", active_status=False))
        drf["flow"]["is_draft"] = True

        norm_pub, hash_pub = get_snapshot_payload_hash(pub)
        norm_drf, hash_drf = get_snapshot_payload_hash(drf)

        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=1,
                op=SnapshotOperation.CREATE.name,
                message=None,
                payload=pub,
                normalized_payload=norm_pub,
                payload_hash=hash_pub,
                role=FlowSnapshotRole.PUBLISHED.value,
            )
        )
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=2,
                op=SnapshotOperation.UPDATE.name,
                message="draft",
                payload=drf,
                normalized_payload=norm_drf,
                payload_hash=hash_drf,
                role=FlowSnapshotRole.DRAFT.value,
            )
        )
        self.session.commit()

        dag_service.update_dag_active_status(flow_id, True)

        draft_row = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id, role=FlowSnapshotRole.DRAFT.value)
            .order_by(FlowSnapshot.version.desc())
            .first()
        )
        self.assertIsNotNone(draft_row)
        self.assertEqual(draft_row.payload["flow"]["description"], "draft-only-body")

        current_row = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id, role=FlowSnapshotRole.PUBLISHED.value)
            .first()
        )
        self.assertIsNotNone(current_row)
        self.assertTrue(current_row.payload["flow"]["active_status"])
        self.assertEqual(current_row.version, 1)
        self.assertEqual(
            self.session.query(FlowSnapshot).filter_by(flow_id=flow_id).count(),
            2,
        )

    def test_legacy_draft_below_current_cleared_on_finalize(self):
        """is_current보다 낮은 버전에 남은 is_draft=True는 finalize 시 false로 정리된다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(id=flow_id, name="Legacy", dag_id="legacy", is_draft=True)
        self.session.add(flow)
        self.session.flush()

        snap1_body = copy.deepcopy(_base_payload(flow_id, "Legacy", "v1"))
        snap2_body = copy.deepcopy(_base_payload(flow_id, "Legacy", "v2"))
        n1, h1 = get_snapshot_payload_hash(snap1_body)
        n2, h2 = get_snapshot_payload_hash(snap2_body)

        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=1,
                op=SnapshotOperation.UPDATE.name,
                message="legacy",
                payload=snap1_body,
                normalized_payload=n1,
                payload_hash=h1,
                role=FlowSnapshotRole.DRAFT.value,
            )
        )
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=2,
                op=SnapshotOperation.CREATE.name,
                message=None,
                payload=snap2_body,
                normalized_payload=n2,
                payload_hash=h2,
                role=FlowSnapshotRole.PUBLISHED.value,
            )
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        svc._finalize_snapshot_write(flow, cleanup_stale_drafts_below_current=True)

        v1 = self.session.query(FlowSnapshot).filter_by(flow_id=flow_id, version=1).one()
        self.assertFalse(v1.is_draft)
        v2 = self.session.query(FlowSnapshot).filter_by(flow_id=flow_id, version=2).one()
        self.assertTrue(v2.is_current)

    def test_save_published_with_existing_draft_keeps_draft_row(self):
        """save_flow_snapshot(PUBLISHED)는 드래프트 행을 삭제하지 않는다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="F2",
            dag_id="f2",
            is_draft=True,
        )
        self.session.add(flow)
        self.session.flush()

        pub = copy.deepcopy(_base_payload(flow_id, "F2", "pub"))
        drf = copy.deepcopy(_base_payload(flow_id, "F2", "draft"))
        drf["flow"]["is_draft"] = True
        np, hp = get_snapshot_payload_hash(pub)
        nd, hd = get_snapshot_payload_hash(drf)

        self.session.add_all(
            [
                FlowSnapshot(
                    flow_id=flow_id,
                    version=1,
                    op=SnapshotOperation.CREATE.name,
                    message=None,
                    payload=pub,
                    normalized_payload=np,
                    payload_hash=hp,
                    role=FlowSnapshotRole.PUBLISHED.value,
                ),
                FlowSnapshot(
                    flow_id=flow_id,
                    version=2,
                    op=SnapshotOperation.UPDATE.name,
                    message="d",
                    payload=drf,
                    normalized_payload=nd,
                    payload_hash=hd,
                    role=FlowSnapshotRole.DRAFT.value,
                ),
            ]
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        new_pub = copy.deepcopy(pub)
        new_pub["flow"]["description"] = "pub-updated"
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            payload=new_pub,
        )
        self.session.commit()

        drafts = self.session.query(FlowSnapshot).filter_by(
            flow_id=flow_id, role=FlowSnapshotRole.DRAFT.value
        ).all()
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0].payload["flow"]["description"], "draft")

    def test_sync_sets_is_draft_true_when_draft_snapshot_exists(self):
        """목록용 flow.is_draft는 드래프트 스냅샷 존재 여부와 일치해야 한다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="SyncDraft",
            dag_id="sync_draft",
            is_draft=False,
        )
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "SyncDraft", "p"))
        drf = copy.deepcopy(_base_payload(flow_id, "SyncDraft", "d"))
        drf["flow"]["is_draft"] = True
        np, hp = get_snapshot_payload_hash(pub)
        nd, hd = get_snapshot_payload_hash(drf)
        self.session.add_all(
            [
                FlowSnapshot(
                    flow_id=flow_id,
                    version=1,
                    op=SnapshotOperation.CREATE.name,
                    message=None,
                    payload=pub,
                    normalized_payload=np,
                    payload_hash=hp,
                    role=FlowSnapshotRole.PUBLISHED.value,
                ),
                FlowSnapshot(
                    flow_id=flow_id,
                    version=2,
                    op=SnapshotOperation.UPDATE.name,
                    message="d",
                    payload=drf,
                    normalized_payload=nd,
                    payload_hash=hd,
                    role=FlowSnapshotRole.DRAFT.value,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(flow)
        flow.is_draft = False
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        self.session.refresh(flow)
        svc._sync_flow_list_from_snapshots(flow)
        self.assertTrue(flow.is_draft)

    def test_sync_clears_is_draft_when_only_published_snapshot(self):
        """드래프트 스냅샷이 없으면 flow.is_draft는 False."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="SyncPub",
            dag_id="sync_pub",
            is_draft=True,
        )
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "SyncPub", "p"))
        np, hp = get_snapshot_payload_hash(pub)
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=1,
                op=SnapshotOperation.CREATE.name,
                message=None,
                payload=pub,
                normalized_payload=np,
                payload_hash=hp,
                role=FlowSnapshotRole.PUBLISHED.value,
            )
        )
        self.session.commit()
        self.session.refresh(flow)

        svc = FlowDefinitionService(self.session)
        svc._sync_flow_list_from_snapshots(flow)
        self.assertFalse(flow.is_draft)

    def test_publish_promotes_lone_draft_row_in_place(self):
        """공개 행이 없을 때 저장(PUBLISHED)하면 최신 드래프트 행을 새 버전 없이 승격한다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="LoneDraft",
            dag_id="lone_draft",
            is_draft=True,
        )
        self.session.add(flow)
        self.session.flush()
        drf = copy.deepcopy(_base_payload(flow_id, "LoneDraft", "will-publish"))
        drf["flow"]["is_draft"] = True
        nd, hd = get_snapshot_payload_hash(drf)
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=1,
                op=SnapshotOperation.DUMMY.name,
                message="d",
                payload=drf,
                normalized_payload=nd,
                payload_hash=hd,
                role=FlowSnapshotRole.DRAFT.value,
            )
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        pub = copy.deepcopy(drf)
        pub["flow"]["is_draft"] = False
        _, hp = get_snapshot_payload_hash(pub)
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            message="publish",
            payload=pub,
        )
        self.session.commit()

        rows = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id)
            .order_by(FlowSnapshot.version)
            .all()
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].version, 1)
        self.assertTrue(rows[0].is_current)
        self.assertFalse(rows[0].is_draft)
        self.assertEqual(rows[0].payload_hash, hp)

    def test_sync_list_meta_from_published_snapshot(self):
        """공개 스냅샷 페이로드가 목록 메타(설명 등)의 출처."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(
            id=flow_id,
            name="OldName",
            dag_id="old_name",
            description="wrong",
            is_draft=False,
        )
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "ListMeta", "from-snapshot"))
        pub["flow"]["name"] = "ListMeta"
        pub["flow"]["dag_id"] = "list_meta"
        np, hp = get_snapshot_payload_hash(pub)
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=1,
                op=SnapshotOperation.CREATE.name,
                message=None,
                payload=pub,
                normalized_payload=np,
                payload_hash=hp,
                role=FlowSnapshotRole.PUBLISHED.value,
            )
        )
        self.session.commit()
        self.session.refresh(flow)

        svc = FlowDefinitionService(self.session)
        svc._sync_flow_list_from_snapshots(flow)
        self.assertEqual(flow.name, "ListMeta")
        self.assertEqual(flow.description, "from-snapshot")
        self.assertEqual(flow.dag_id, "list_meta")

    def test_publish_payload_equal_to_draft_promotes_draft_row(self):
        """v1 공개 + v2 드래프트일 때, 공개 저장 페이로드가 드래프트와 같으면 새 버전 없이 v2 승격."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(id=flow_id, name="Promo", dag_id="promo", is_draft=True)
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "Promo", "pub-body"))
        drf = copy.deepcopy(_base_payload(flow_id, "Promo", "draft-body"))
        drf["flow"]["is_draft"] = True
        np, hp = get_snapshot_payload_hash(pub)
        nd, hd = get_snapshot_payload_hash(drf)
        self.session.add_all(
            [
                FlowSnapshot(
                    flow_id=flow_id,
                    version=1,
                    op=SnapshotOperation.CREATE.name,
                    message=None,
                    payload=pub,
                    normalized_payload=np,
                    payload_hash=hp,
                    role=FlowSnapshotRole.PUBLISHED.value,
                ),
                FlowSnapshot(
                    flow_id=flow_id,
                    version=2,
                    op=SnapshotOperation.UPDATE.name,
                    message="d",
                    payload=drf,
                    normalized_payload=nd,
                    payload_hash=hd,
                    role=FlowSnapshotRole.DRAFT.value,
                ),
            ]
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        publish_payload = copy.deepcopy(drf)
        publish_payload["flow"]["is_draft"] = False
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            message="publish-draft",
            payload=publish_payload,
        )
        self.session.commit()

        rows = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id)
            .order_by(FlowSnapshot.version)
            .all()
        )
        self.assertEqual(len(rows), 2)
        self.assertFalse(rows[0].is_current)
        self.assertTrue(rows[1].is_current)
        self.assertFalse(rows[1].is_draft)
        self.assertEqual(rows[1].version, 2)
        self.assertEqual(rows[1].payload["flow"]["description"], "draft-body")

    def test_draft_save_from_current_only_creates_higher_version_draft(self):
        """공개만 있을 때(예: v2 current) 임시저장으로 내용이 바뀌면 더 큰 버전의 draft 행이 생긴다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(id=flow_id, name="OnlyCur", dag_id="only_cur", is_draft=False)
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "OnlyCur", "v2-published"))
        np, hp = get_snapshot_payload_hash(pub)
        self.session.add(
            FlowSnapshot(
                flow_id=flow_id,
                version=2,
                op=SnapshotOperation.CREATE.name,
                message=None,
                payload=pub,
                normalized_payload=np,
                payload_hash=hp,
                role=FlowSnapshotRole.PUBLISHED.value,
            )
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        draft_payload = copy.deepcopy(pub)
        draft_payload["flow"]["description"] = "edited-in-draft"
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.DRAFT,
            message="tmp",
            payload=draft_payload,
        )
        self.session.commit()

        rows = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id)
            .order_by(FlowSnapshot.version)
            .all()
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].version, 2)
        self.assertTrue(rows[0].is_current)
        self.assertEqual(rows[1].version, 3)
        self.assertTrue(rows[1].is_draft)
        self.assertEqual(rows[1].payload["flow"]["description"], "edited-in-draft")

    def test_draft_save_updates_existing_row_in_place(self):
        """기존 draft 행이 있으면 임시저장은 같은 version row만 갱신한다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(id=flow_id, name="InPlace", dag_id="in_place", is_draft=True)
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "InPlace", "pub"))
        drf = copy.deepcopy(_base_payload(flow_id, "InPlace", "draft-v1"))
        drf["flow"]["is_draft"] = True
        np, hp = get_snapshot_payload_hash(pub)
        nd, hd = get_snapshot_payload_hash(drf)
        self.session.add_all(
            [
                FlowSnapshot(
                    flow_id=flow_id,
                    version=1,
                    op=SnapshotOperation.CREATE.name,
                    message=None,
                    payload=pub,
                    normalized_payload=np,
                    payload_hash=hp,
                    role=FlowSnapshotRole.PUBLISHED.value,
                ),
                FlowSnapshot(
                    flow_id=flow_id,
                    version=2,
                    op=SnapshotOperation.UPDATE.name,
                    message="d",
                    payload=drf,
                    normalized_payload=nd,
                    payload_hash=hd,
                    role=FlowSnapshotRole.DRAFT.value,
                ),
            ]
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        drf2 = copy.deepcopy(drf)
        drf2["flow"]["description"] = "draft-v2"
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.DRAFT,
            message="edit",
            payload=drf2,
        )
        self.session.commit()

        rows = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id)
            .order_by(FlowSnapshot.version)
            .all()
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1].version, 2)
        self.assertTrue(rows[1].is_draft)
        self.assertEqual(rows[1].payload["flow"]["description"], "draft-v2")

    def test_draft_noop_deletes_redundant_draft_when_same_as_published(self):
        """draft 내용이 공개와 동일하면 임시저장 noop 시 draft 행을 제거한다."""
        flow_id = str(uuid.uuid4())
        flow = DBFlow(id=flow_id, name="Redundant", dag_id="redundant", is_draft=True)
        self.session.add(flow)
        self.session.flush()
        pub = copy.deepcopy(_base_payload(flow_id, "Redundant", "same-body"))
        drf = copy.deepcopy(_base_payload(flow_id, "Redundant", "same-body"))
        drf["flow"]["is_draft"] = True
        np, hp = get_snapshot_payload_hash(pub)
        nd, hd = get_snapshot_payload_hash(drf)
        self.assertEqual(hp, hd)
        self.session.add_all(
            [
                FlowSnapshot(
                    flow_id=flow_id,
                    version=1,
                    op=SnapshotOperation.CREATE.name,
                    message=None,
                    payload=pub,
                    normalized_payload=np,
                    payload_hash=hp,
                    role=FlowSnapshotRole.PUBLISHED.value,
                ),
                FlowSnapshot(
                    flow_id=flow_id,
                    version=2,
                    op=SnapshotOperation.UPDATE.name,
                    message="d",
                    payload=drf,
                    normalized_payload=nd,
                    payload_hash=hd,
                    role=FlowSnapshotRole.DRAFT.value,
                ),
            ]
        )
        self.session.commit()

        svc = FlowDefinitionService(self.session)
        same = copy.deepcopy(drf)
        svc.save_flow_snapshot(
            flow,
            SnapshotOperation.UPDATE,
            snapshot_target=SnapshotTarget.DRAFT,
            message="noop",
            payload=same,
        )
        self.session.commit()

        rows = (
            self.session.query(FlowSnapshot)
            .filter_by(flow_id=flow_id)
            .order_by(FlowSnapshot.version)
            .all()
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].version, 1)
        self.assertTrue(rows[0].is_current)


if __name__ == "__main__":
    unittest.main()
