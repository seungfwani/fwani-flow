import copy
import datetime
import logging
import shutil
from pathlib import Path

import requests
from sqlalchemy import or_, and_, func, asc, desc, inspect, literal
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.operators import like_op

from config import Config
from core.airflow_client import AirflowClient
from core.snapshot import (
    SnapshotOperation,
    SnapshotTarget,
    get_snapshot_payload_hash,
    build_flow_snapshot_by_domain,
    build_minimal_payload_from_flow,
)
from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.flow import Flow as DBFlow, FlowSnapshot
from models.db.flow_snapshot_role import FlowSnapshotRole
from models.db.flow_execution_queue import FlowExecutionQueue
from models.db.keycloak_mapper import KeycloakUserEntity
from models.domain.execution_status_presenter import get_execution_status_presenter
from models.domain.mapper import (
    flow_api2domain,
    flow_snapshot2api,
    flow_to_dag_list_response,
    payload_to_domain_flow,
)
from utils.functions import make_flow_id_by_name, to_snake

logger = logging.getLogger()


class FlowDefinitionService:
    def __init__(self, meta_db: Session, airflow_db: Session = None, airflow_client: AirflowClient = None):
        self.meta_db = meta_db
        self.airflow_db = airflow_db
        self.airflow_client = airflow_client

    def _get_flow(self, dag_id: str) -> DBFlow:
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow:
            raise WorkflowError(f"Flow({dag_id}) not found")
        return flow

    def find_existing_flow(self, dag_name: str) -> DBFlow:
        return (
            self.meta_db.query(DBFlow)
            # .filter((DBFlow.id == self.dag.id) | (DBFlow.name == self.dag.name))
            .filter(and_(DBFlow.name == dag_name))
            .first()
        )

    def check_available_dag_name(self, dag_name: str) -> bool:
        flow = self.find_existing_flow(dag_name=dag_name)
        if flow:
            return False
        else:
            return True

    def next_version(self, flow_id: str) -> int:
        cur = (self.meta_db.query(func.max(FlowSnapshot.version))
               .filter(FlowSnapshot.flow_id == flow_id)
               .scalar())
        return (cur or 0) + 1

    def _demote_published_snapshots(self, flow_id: str) -> None:
        self.meta_db.query(FlowSnapshot).filter(
            FlowSnapshot.flow_id == flow_id,
            FlowSnapshot.role == FlowSnapshotRole.PUBLISHED.value,
        ).update({"role": FlowSnapshotRole.ARCHIVED.value}, synchronize_session=False)

    def _draft_snapshot_exists(self, flow_id: str) -> bool:
        return (
            self.meta_db.query(FlowSnapshot.id)
            .filter(
                FlowSnapshot.flow_id == flow_id,
                FlowSnapshot.role == FlowSnapshotRole.DRAFT.value,
            )
            .first()
            is not None
        )

    def _sync_flow_list_from_snapshots(self, db_flow: DBFlow) -> None:
        """목록용 flow 행을 스냅샷과 맞춘다. 공개본(role=published) 페이로드가 메타 단일 출처, Flow.is_draft는 드래프트 행 존재 여부."""
        draft_exists = self._draft_snapshot_exists(db_flow.id)
        current = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id, role=FlowSnapshotRole.PUBLISHED.value)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        if current:
            f = current.payload["flow"]
            db_flow.name = f["name"]
            db_flow.dag_id = f.get("dag_id") or db_flow.dag_id
            db_flow.description = f.get("description")
            db_flow.owner_id = f.get("owner_id")
            db_flow.schedule = f.get("schedule")
            db_flow.schedule_options = f.get("schedule_options")
            db_flow.max_retries = f.get("max_retries", 0) or 0
            db_flow.active_status = f.get("active_status", False)
            db_flow.is_deleted = f.get("is_deleted", False)
            h = f.get("hash")
            if h is not None:
                db_flow.hash = str(h)
            if "is_loaded_by_airflow" in f:
                db_flow.is_loaded_by_airflow = f["is_loaded_by_airflow"]
        db_flow.is_draft = draft_exists

    def _clear_stale_draft_flags_below_current(
        self,
        db_flow: DBFlow,
        *,
        publish_supersedes_version: int | None = None,
    ) -> None:
        """레거시 draft 행을 archived로 정리한다.

        - publish_supersedes_version 이 None이면: 공개 헤드보다 낮은 버전의 draft 전부 archived.
        - 새 공개 행을 추가해 헤드가 올라간 경우: 이전 공개 헤드 버전(publish_supersedes_version) 이하의 draft만
          archived 해서, 그보다 큰 버전의 워킹 드래프트는 유지한다.
        """
        current = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id, role=FlowSnapshotRole.PUBLISHED.value)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        if current is None:
            return
        q = self.meta_db.query(FlowSnapshot).filter(
            FlowSnapshot.flow_id == db_flow.id,
            FlowSnapshot.version < current.version,
            FlowSnapshot.role == FlowSnapshotRole.DRAFT.value,
        )
        if publish_supersedes_version is not None:
            q = q.filter(FlowSnapshot.version <= publish_supersedes_version)
        n = q.update({"role": FlowSnapshotRole.ARCHIVED.value}, synchronize_session=False)
        if n:
            extra = (
                f", cutoff<=v{publish_supersedes_version}"
                if publish_supersedes_version is not None
                else ""
            )
            logger.info(
                f"🧹 Set role=archived on {n} stale draft row(s) (published v{current.version}{extra})"
            )

    def _finalize_snapshot_write(
        self,
        db_flow: DBFlow,
        *,
        cleanup_stale_drafts_below_current: bool = False,
        publish_supersedes_version: int | None = None,
    ) -> None:
        self.meta_db.flush()
        if cleanup_stale_drafts_below_current:
            self._clear_stale_draft_flags_below_current(
                db_flow, publish_supersedes_version=publish_supersedes_version
            )
        self._sync_flow_list_from_snapshots(db_flow)

    def save_flow_snapshot(self,
                           db_flow: DBFlow,
                           op: SnapshotOperation,
                           snapshot_target: SnapshotTarget,
                           message: str | None = None,
                           upsert_draft: bool = True,
                           payload: dict = None,
                           ):
        if payload is None:
            raise ValueError("save_flow_snapshot requires payload")
        is_draft_slot = snapshot_target == SnapshotTarget.DRAFT
        payload["flow"]["is_draft"] = is_draft_slot
        payload["flow"]["active_status"] = getattr(db_flow, "active_status", False)
        normalized_payload, payload_hash = get_snapshot_payload_hash(payload)

        current_snap = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id, role=FlowSnapshotRole.PUBLISHED.value)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        draft_snap = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id, role=FlowSnapshotRole.DRAFT.value)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        last_snap = (
            self.meta_db.query(FlowSnapshot)
            .filter(FlowSnapshot.flow_id == db_flow.id)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        if last_snap:
            logger.info(f"🔄 last hash: {last_snap.payload_hash}, new hash: {payload_hash}")
        else:
            logger.info(f"🆕 new hash: {payload_hash}")

        def _append_message(row: FlowSnapshot) -> None:
            row.message = (row.message or "") + "\n" + (message or "")

        if is_draft_slot:
            if draft_snap is not None:
                if draft_snap.payload_hash == payload_hash:
                    if current_snap and current_snap.payload_hash == payload_hash:
                        logger.info(
                            "🤷 Draft equals stored draft and published; drop redundant draft."
                        )
                        if upsert_draft:
                            self.meta_db.delete(draft_snap)
                        self._finalize_snapshot_write(db_flow)
                        return current_snap, False
                    logger.info("🤷 No changes detected vs existing draft.")
                    self._finalize_snapshot_write(db_flow)
                    return draft_snap, False
                if (
                    draft_snap.op == SnapshotOperation.DUMMY.name
                    and not payload.get("tasks", [])
                ):
                    logger.info("🤷 No changes detected from Dummy.")
                    self._finalize_snapshot_write(db_flow)
                    return draft_snap, False
                logger.info(f"📝 Update draft snapshot v{draft_snap.version}.")
                draft_snap.op = op.name
                draft_snap.message = message
                draft_snap.payload = payload
                draft_snap.normalized_payload = normalized_payload
                draft_snap.payload_hash = payload_hash
                self._finalize_snapshot_write(db_flow)
                return draft_snap, True

            if current_snap and current_snap.payload_hash == payload_hash:
                logger.info("🤷 Draft payload matches published; drop redundant draft if any.")
                if upsert_draft:
                    self.meta_db.query(FlowSnapshot).filter_by(
                        flow_id=db_flow.id, role=FlowSnapshotRole.DRAFT.value
                    ).delete()
                self._finalize_snapshot_write(db_flow)
                return current_snap, False
            if last_snap and last_snap.op == SnapshotOperation.DUMMY.name and not payload.get(
                "tasks", []
            ):
                logger.info("🤷 No changes detected from Dummy.")
                self._finalize_snapshot_write(db_flow)
                return last_snap, False

            if upsert_draft:
                self.meta_db.query(FlowSnapshot).filter_by(
                    flow_id=db_flow.id, role=FlowSnapshotRole.DRAFT.value
                ).delete()
            new_version = self.next_version(db_flow.id)
            snap = FlowSnapshot(
                flow=db_flow,
                version=new_version,
                op=op.name,
                message=message,
                payload=payload,
                normalized_payload=normalized_payload,
                payload_hash=payload_hash,
                role=FlowSnapshotRole.DRAFT.value,
            )
            logger.info(f"🆕 Create new draft snapshot to {snap.version}.")
            self.meta_db.add(snap)
            self._finalize_snapshot_write(db_flow)
            return snap, True

        if current_snap is None and draft_snap is not None:
            logger.info(f"▶️ Promote lone draft v{draft_snap.version} to published.")
            self._demote_published_snapshots(db_flow.id)
            draft_snap.role = FlowSnapshotRole.PUBLISHED.value
            draft_snap.op = op.name
            _append_message(draft_snap)
            draft_snap.payload = payload
            draft_snap.normalized_payload = normalized_payload
            draft_snap.payload_hash = payload_hash
            self._finalize_snapshot_write(
                db_flow, cleanup_stale_drafts_below_current=True
            )
            return draft_snap, True

        if current_snap is not None and current_snap.payload_hash == payload_hash:
            logger.info("🤷 No changes detected vs published (current).")
            self._finalize_snapshot_write(db_flow)
            return current_snap, False

        if draft_snap is not None and draft_snap.payload_hash == payload_hash:
            logger.info(f"▶️ Publish: promote draft v{draft_snap.version} (same payload hash).")
            superseded_pub_ver = current_snap.version
            self._demote_published_snapshots(db_flow.id)
            draft_snap.role = FlowSnapshotRole.PUBLISHED.value
            draft_snap.op = op.name
            _append_message(draft_snap)
            draft_snap.payload = payload
            draft_snap.normalized_payload = normalized_payload
            draft_snap.payload_hash = payload_hash
            self._finalize_snapshot_write(
                db_flow,
                cleanup_stale_drafts_below_current=True,
                publish_supersedes_version=superseded_pub_ver,
            )
            return draft_snap, True

        superseded_pub_ver = current_snap.version
        self._demote_published_snapshots(db_flow.id)
        new_version = self.next_version(db_flow.id)
        payload["flow"]["active_status"] = False
        snap = FlowSnapshot(
            flow=db_flow,
            version=new_version,
            op=op.name,
            message=message,
            payload=payload,
            normalized_payload=normalized_payload,
            payload_hash=payload_hash,
            role=FlowSnapshotRole.PUBLISHED.value,
        )
        logger.info(f"🆕 Create new published snapshot to {snap.version}.")
        self.meta_db.add(snap)
        self._finalize_snapshot_write(
            db_flow,
            cleanup_stale_drafts_below_current=True,
            publish_supersedes_version=superseded_pub_ver,
        )
        return snap, True

    def restore_flow_by_snapshot(self, flow_id: str, version: int):
        logger.info(f"▶️ Start to restore snapshot {version}.")
        fs = (self.meta_db.query(FlowSnapshot)
              .filter_by(flow_id=flow_id, version=version)
              .one())
        data = copy.deepcopy(fs.payload)
        flow = self._get_flow(flow_id)
        self.save_flow_snapshot(
            flow,
            op=SnapshotOperation.RESTORE,
            snapshot_target=SnapshotTarget.DRAFT if fs.role == FlowSnapshotRole.DRAFT.value else SnapshotTarget.PUBLISHED,
            message=f"이전 버전 restore - v{version}",
            payload=data,
        )
        self.meta_db.commit()
        logger.info(f"✅ Success to restore snapshot {version}.")
        return flow.id

    def create_dummy(self, owner_id: str = None):
        logger.info(f"🆕 Create dummy flow")
        now_timestamp = datetime.datetime.now(datetime.timezone.utc)

        name = "Workflow_" + now_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        dummy_flow = DBFlow(
            name=name,
            dag_id=make_flow_id_by_name(name),
            owner_id=owner_id,
            is_draft=True,
        )
        self.meta_db.add(dummy_flow)
        self.meta_db.flush()
        snap, _ = self.save_flow_snapshot(
            dummy_flow,
            SnapshotOperation.DUMMY,
            snapshot_target=SnapshotTarget.DRAFT,
            message=Config.DUMMY_MSG,
            payload=build_minimal_payload_from_flow(dummy_flow),
        )
        self.meta_db.commit()
        return payload_to_domain_flow(snap.payload)

    @staticmethod
    def _build_workflow_dag_save_node(task: dict) -> dict:
        """meta-type/workflow-dag/save 요청용 노드 한 건 (params / metaTypeIds 분리)."""
        raw = {item["key"]: item["value"] for item in task.get("inputs", [])}
        meta_type_ids = raw.get("metaTypeIds") or []
        properties_raw = raw.get("properties") or []
        params = {
            "id": raw.get("id"),
            "name": raw.get("name"),
            "description": raw.get("description"),
            "ownerId": raw.get("ownerId"),
            "connectionInstanceId": raw.get("connectionInstanceId"),
            "schemaName": raw.get("schemaName") or raw.get("metaTypeSchemaName"),
            "tagIds": raw.get("tagIds") or [],
            "properties": [
                {
                    "metaTypePropertyName": p.get("name"),
                    "description": p.get("description"),
                    "dataType": p.get("dataType"),
                }
                for p in properties_raw
            ],
        }
        return {
            "nodeId": task["id"],
            "params": params,
            "metaTypeIds": copy.deepcopy(meta_type_ids),
        }

    def _notify_metatype_dag_schema(self, flow_id: str, payload: dict) -> None:
        tasks = payload.get("tasks", [])
        meta_tasks = [t for t in tasks if t.get("kind") == "meta"]
        if not meta_tasks:
            return

        nodes = []
        host = None
        for task in meta_tasks:
            raw = {item["key"]: item["value"] for item in task.get("inputs", [])}
            if host is None:
                host = raw.get("host")
            nodes.append(self._build_workflow_dag_save_node(task))

        if not host:
            logger.warning("⚠️ No host found in meta node params, skip metatype dag schema notification")
            return

        url = f"{host.rstrip('/')}/graphio/v1/meta-type/workflow-dag/save"
        body = {
            "workflowId": flow_id,
            "nodes": nodes,
        }

        try:
            resp = requests.post(url, json=body, timeout=10)
            if resp.ok:
                logger.info(f"✅ Metatype dag schema saved: {resp.status_code}")
            else:
                logger.warning(f"⚠️ Metatype dag schema save failed: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.warning(f"⚠️ Metatype dag schema save request failed: {e}")

    def save_dag(self, dag: DAGRequest):
        existing = self.find_existing_flow(dag.name)
        if existing:
            raise WorkflowError("DAG already exists")
        logger.info(f"🆕 Create New DAG: {dag.name}")
        domain_flow = flow_api2domain(dag)
        flow_id = str(__import__("uuid").uuid4())
        payload = build_flow_snapshot_by_domain(domain_flow, flow_id)
        db_flow = DBFlow(
            id=flow_id,
            name=domain_flow.name,
            dag_id=domain_flow.dag_id,
            description=domain_flow.description,
            owner_id=domain_flow.owner_id,
            schedule=domain_flow.scheduled,
            schedule_options=domain_flow.schedule_options,
            max_retries=domain_flow.max_retries,
            is_draft=domain_flow.is_draft,
        )
        self.meta_db.add(db_flow)
        self.meta_db.flush()
        _, is_snap_changed = self.save_flow_snapshot(
            db_flow,
            SnapshotOperation.CREATE,
            snapshot_target=SnapshotTarget.DRAFT if dag.is_draft else SnapshotTarget.PUBLISHED,
            message="신규 등록",
            payload=payload,
        )
        if is_snap_changed:
            domain_for_file = payload_to_domain_flow(payload)
            domain_for_file.write_file()
            db_flow.file_hash = domain_for_file.file_hash
        self.meta_db.commit()
        # dag를 저장한 뒤 ontology에 메타타입과 메타매핑을 저장
        self._notify_metatype_dag_schema(flow_id, payload)
        snap = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id)
            .order_by(desc(FlowSnapshot.version))
            .first()
        )
        return payload_to_domain_flow(snap.payload)

    def update_dag(self, origin_dag_id: str, new_dag: DAGRequest):
        if not new_dag:
            logger.info(f"🤷 No dag to update. Do nothing.")
            return None
        # 0. 기존 Flow 조회
        origin_flow = self._get_flow(origin_dag_id)

        # 1. 변환
        new_flow = flow_api2domain(new_dag, origin_dag_id)
        if new_flow.is_draft:  # 수정중
            # snapshot 만 저장
            draft_payload = build_flow_snapshot_by_domain(new_flow, origin_dag_id)
            snap, is_snap_changed = self.save_flow_snapshot(
                origin_flow,
                SnapshotOperation.UPDATE,
                snapshot_target=SnapshotTarget.DRAFT,
                message="draft=True",
                payload=draft_payload,
            )
            if not is_snap_changed and snap.op == SnapshotOperation.DUMMY.name:
                if not snap.payload.get("tasks", []):
                    logger.warning("🧹 Delete unchanged Dummy Flow")
                    self.meta_db.delete(origin_flow)
            self.meta_db.commit()
            self._notify_metatype_dag_schema(origin_dag_id, draft_payload)
            return new_flow
        # 저장시(is_draft=False) 이름이 바뀐 경우 → 중복 확인 및 갱신
        if new_flow.name != origin_flow.name:
            logger.info(f"▶️ Check duplicated name {new_flow.name}.")
            duplicate = (
                self.meta_db.query(DBFlow)
                .filter(DBFlow.name == new_flow.name, DBFlow.id != origin_flow.id)
                .first()
            )
            if duplicate:
                raise WorkflowError(f"Flow 이름 '{new_flow.name}' 은 이미 존재합니다.")

        payload = build_flow_snapshot_by_domain(new_flow, origin_dag_id)
        try:
            snap, is_snap_changed = self.save_flow_snapshot(
                origin_flow,
                SnapshotOperation.UPDATE,
                snapshot_target=SnapshotTarget.PUBLISHED,
                message="필드 수정",
                payload=payload,
            )
            if is_snap_changed:
                domain_for_file = payload_to_domain_flow(payload)
                domain_for_file.write_file()
                origin_flow.file_hash = domain_for_file.file_hash
            self.meta_db.commit()
            self._notify_metatype_dag_schema(origin_dag_id, payload)
            return payload_to_domain_flow(snap.payload)
        except Exception as e:
            self.meta_db.rollback()
            raise WorkflowError(f"❌ DAG 업데이트 실패: {e}")

    def _get_current_snapshot_payload(self, db_flow: DBFlow) -> dict:
        current = (
            self.meta_db.query(FlowSnapshot)
            .filter_by(flow_id=db_flow.id, role=FlowSnapshotRole.PUBLISHED.value)
            .first()
        )
        if current:
            return current.payload
        return build_minimal_payload_from_flow(db_flow)

    def update_dag_active_status(self, dag_id: str, active_status: bool) -> bool:
        flow = self._get_flow(dag_id)
        try:
            result = self.airflow_client.update_pause(flow.dag_id, False if active_status else True)
            logger.info(f"🔄 Update airflow is_paused to '{result}'")
            flow.active_status = active_status
            current_snap = (
                self.meta_db.query(FlowSnapshot)
                .filter_by(flow_id=flow.id, role=FlowSnapshotRole.PUBLISHED.value)
                .order_by(desc(FlowSnapshot.version))
                .first()
            )
            if current_snap is not None:
                payload = copy.deepcopy(current_snap.payload)
                payload["flow"]["active_status"] = active_status
                normalized_payload, payload_hash = get_snapshot_payload_hash(payload)
                current_snap.payload = payload
                current_snap.normalized_payload = normalized_payload
                current_snap.payload_hash = payload_hash
                self._finalize_snapshot_write(flow)
            else:
                payload = self._get_current_snapshot_payload(flow)
                payload["flow"]["active_status"] = active_status
                self.save_flow_snapshot(
                    flow,
                    SnapshotOperation.UPDATE,
                    snapshot_target=SnapshotTarget.PUBLISHED,
                    message="activate status 수정",
                    payload=payload,
                )
        except Exception as e:
            logger.warning(f"❌ Failed to update airflow is_paused to '{active_status}'", exc_info=e)
        self.meta_db.commit()
        return flow.active_status

    def get_dag_total_count(self):
        return (self.meta_db.query(func.count(DBFlow.id))
                .filter(~DBFlow.flow_snapshots.any(FlowSnapshot.op == "DUMMY"))
                .filter(DBFlow.is_deleted == False).scalar())

    def get_active_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).filter(DBFlow.is_deleted == False).all()

    def get_all_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).all()

    def get_dag_list(self,
                     active_status: set[bool],
                     execution_status: set[str],
                     owner: set[str],
                     name: str,
                     sort: str,
                     offset: int = 0,
                     limit: int = 10,
                     include_deleted=False):
        logger.info(f"▶️ Get dag list filter:"
                    f" active_status={active_status},"
                    f" execution_status={execution_status},"
                    f" owner={owner},"
                    f" name={name},"
                    f" sort={sort},"
                    f" offset={offset},"
                    f" limit={limit},"
                    f" include_deleted={include_deleted}")
        FEQ1 = aliased(FlowExecutionQueue)
        FEQ2 = aliased(FlowExecutionQueue)
        FS = aliased(FlowSnapshot)
        current_snapshots = (
            self.meta_db.query(
                FS.flow_id.label("flow_id"),
                FS.id.label("snapshot_id")
            )
            .filter(FS.role == FlowSnapshotRole.PUBLISHED.value)
            .subquery()
        )
        subquery = (self.meta_db
                    .query(FEQ1.flow_id.label("flow_id"),
                           func.max(FEQ1.updated_at).label("updated_at"))
                    .join(current_snapshots, FEQ1.flow_snapshot_id == current_snapshots.c.snapshot_id)
                    .group_by(FEQ1.flow_id)
                    .subquery())
        query = (self.meta_db.query(DBFlow, FEQ2.status.label("execution_status"))
                 .outerjoin(subquery, DBFlow.id == subquery.c.flow_id)
                 .outerjoin(FEQ2, and_(FEQ2.flow_id == subquery.c.flow_id,
                                       FEQ2.updated_at == subquery.c.updated_at)))
        query = query.filter(~DBFlow.flow_snapshots.any(FlowSnapshot.op == "DUMMY"))
        if execution_status:
            expanded = get_execution_status_presenter().expand_filter_values(execution_status)
            if expanded:
                query = query.filter(FEQ2.status.in_(expanded))
            else:
                query = query.filter(literal(False))
        if active_status:
            query = query.filter(or_(*[DBFlow.active_status == i for i in active_status]))
        if owner:
            query = query.filter(DBFlow.owner_id.in_(owner))
        if name:
            query = query.filter(like_op(DBFlow.name, f"%{name}%"))
        if sort:
            field, direction = sort.split("_")
            if field == "owner":
                insp = inspect(self.meta_db.bind)
                if insp.has_table("user_entity", schema="keycloak"):
                    column_ = func.coalesce(KeycloakUserEntity.username, literal(None)).label("owner_name")
                    query = (query
                             .outerjoin(KeycloakUserEntity, DBFlow.owner_id == KeycloakUserEntity.id)
                             .with_entities(DBFlow, FEQ2.status.label("execution_status"))
                             )
                    if direction.lower() == "asc":
                        query = query.order_by(asc(column_))
                    elif direction.lower() == "desc":
                        query = query.order_by(desc(column_))
            else:
                field = to_snake(field)
                column_attr = getattr(DBFlow, field, None)
                if column_attr:
                    if direction.lower() == "asc":
                        query = query.order_by(asc(column_attr))
                    elif direction.lower() == "desc":
                        query = query.order_by(desc(column_attr))
        if not include_deleted:
            query = query.filter(DBFlow.is_deleted == False)

        # limit 전 토탈 카운트 체크
        total_count = self.get_dag_total_count()
        filtered_count = (query
                          .order_by(None)  # 불필요한 ORDER BY 제거 (성능)
                          .with_entities(DBFlow.id)  # id 컬럼만 선택
                          .distinct()
                          .count())

        query = query.offset(offset).limit(limit)
        rows = query.all()
        result_count = len(rows)
        if not rows:
            return [], result_count, filtered_count, total_count
        flow_ids = [r[0].id for r in rows]
        display_pairs = (
            self.meta_db.query(FlowSnapshot.flow_id)
            .filter(
                FlowSnapshot.flow_id.in_(flow_ids),
                FlowSnapshot.role.in_(
                    (FlowSnapshotRole.PUBLISHED.value, FlowSnapshotRole.DRAFT.value)
                ),
            )
            .distinct()
            .all()
        )
        valid_flow_ids = {row[0] for row in display_pairs}
        result = []
        for db_flow, execution_status in rows:
            if db_flow.id not in valid_flow_ids:
                continue
            result.append(flow_to_dag_list_response(db_flow, execution_status))
        return result, len(result), filtered_count, total_count

    def get_dag_owner_list(self):
        dag_list = self.meta_db.query(DBFlow).filter(DBFlow.is_deleted == False)

        # owner_id 중복 제거한 목록 조회
        query = (
            dag_list.with_entities(DBFlow.owner_id)
            .filter(DBFlow.owner_id.isnot(None))
            .distinct()
        )

        owner_ids: list[str] = [row.owner_id for row in query.all()]
        return owner_ids

    def get_dag(self, dag_id):
        query = (self.meta_db.query(FlowSnapshot)
                 .filter(FlowSnapshot.flow_id == dag_id)
                 .order_by(desc(FlowSnapshot.version)))
        current_flow = query.filter(
            FlowSnapshot.role == FlowSnapshotRole.PUBLISHED.value
        ).first()
        draft_flow = query.filter(FlowSnapshot.role == FlowSnapshotRole.DRAFT.value).first()
        return flow_snapshot2api(current_flow), flow_snapshot2api(draft_flow)

    def get_snapshot_list(self, dag_id):
        return [{
            "id": snap.id,
            "version": snap.version,
            "is_current": snap.is_current,
            "is_draft": snap.is_draft,
        } for snap in self.meta_db.query(FlowSnapshot)
        .filter(FlowSnapshot.flow_id == dag_id)
        .order_by(desc(FlowSnapshot.version))
        .all()]

    def delete_dag_temporary(self, dag_id: str):
        flow = self._get_flow(dag_id)
        flow.is_deleted = True
        flow.file_hash = None
        payload = self._get_current_snapshot_payload(flow)
        payload["flow"]["is_deleted"] = True
        payload["flow"]["file_hash"] = None
        self.save_flow_snapshot(
            flow,
            SnapshotOperation.DELETE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            message="임시 삭제",
            payload=payload,
        )
        self.meta_db.commit()

        delete_dag_file(flow)
        logger.info(f"🧹 Complete to delete dag temporary: {flow.name}")
        return dag_id

    def delete_dag_permanently(self, dag_id: str):
        flow = self._get_flow(dag_id)
        payload = self._get_current_snapshot_payload(flow)
        payload["flow"]["is_deleted"] = True
        self.save_flow_snapshot(
            flow,
            SnapshotOperation.DELETE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            message="완전 삭제",
            payload=payload,
        )
        self.meta_db.delete(flow)
        self.meta_db.commit()

        delete_dag_file(flow)
        logger.info(f"🧹 Complete to delete dag permanently: {flow.name}")
        return dag_id

    def restore_deleted_dag(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow or not flow.is_deleted:
            raise WorkflowError(f"Flow({dag_id}) not found")
        flow.is_deleted = False
        payload = self._get_current_snapshot_payload(flow)
        payload["flow"]["is_deleted"] = False
        domain_flow = payload_to_domain_flow(payload)
        flow.file_hash = domain_flow.file_hash
        self.save_flow_snapshot(
            flow,
            SnapshotOperation.RESTORE,
            snapshot_target=SnapshotTarget.PUBLISHED,
            message="임시 삭제 flow 복구",
            payload=payload,
        )
        self.meta_db.commit()
        logger.info(f"♻️ Complete to restore DAG: {flow.name}")
        return flow.id


def delete_dag_file(flow: DBFlow):
    logger.info(f"▶️ Start to delete dag file {flow.id}")
    base_path = Path(Config.DAG_DIR)
    # 디렉토리 삭제 시도
    try:
        for folder in base_path.glob(f"{flow.dag_id}*"):  # 최대 두개 나옴, pub버전, draft버전
            if folder.is_dir():
                shutil.rmtree(folder)
                logger.info(f"🧹 Delete DAG directory: {folder}")
        logger.info(f"🧹 Complete to delete directory: {flow.id}")
    except FileNotFoundError:
        logger.warning(f"⚠️ No DAG directory: {flow.id}")
    except Exception as e:
        logger.error(f"❌ Failed to delete DAG({flow.id}) directory: {e}")
