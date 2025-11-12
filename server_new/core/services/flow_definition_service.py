import datetime
import logging
import shutil
from pathlib import Path

from sqlalchemy import or_, and_, func, asc, desc, inspect, literal
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.operators import like_op

from config import Config
from core.airflow_client import AirflowClient
from core.snapshot import build_flow_snapshot, SnapshotOperation, get_snapshot_payload_hash, \
    build_flow_snapshot_by_domain
from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.edge import Edge as DBEdge
from models.db.flow import Flow as DBFlow, FlowSnapshot
from models.db.flow_execution_queue import FlowExecutionQueue
from models.db.keycloak_mapper import KeycloakUserEntity
from models.db.task import Task as DBTask, TaskInput
from models.domain.mapper import flow_api2domain, flow_db2domain, flow_domain2db, task_edge_domain2db, flow_snapshot2api
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

    def save_flow_snapshot(self,
                           db_flow: DBFlow,
                           op: SnapshotOperation,
                           message: str | None = None,
                           is_draft: bool = False,
                           upsert_draft: bool = True,
                           payload: dict = None,
                           ):
        # 스냅샷 페이로드 & 해시 생성
        if not payload:
            payload = build_flow_snapshot(db_flow)
        normalized_payload, payload_hash = get_snapshot_payload_hash(payload)

        # 변경점 확인 최적화
        last_snap = (self.meta_db.query(FlowSnapshot)
                     .filter(FlowSnapshot.flow_id == db_flow.id)
                     .order_by(desc(FlowSnapshot.version))
                     .first())
        if last_snap:
            logger.info(f"🔄 last hash: {last_snap.payload_hash}, new hash: {payload_hash}")
            if last_snap.is_current and last_snap.payload_hash == payload_hash:
                logger.info(
                    f"🤷 No changes detected from current.")
                return last_snap, False
            elif last_snap.op == SnapshotOperation.DUMMY.name:
                if is_draft and not payload.get("tasks", []):
                    logger.info("🤷 No changes detected from Dummy.")
                    return last_snap, False
        else:
            logger.info(f"🆕 new hash: {payload_hash}")

        # draft/current 정리
        if is_draft and upsert_draft:
            logger.info("🧹 Delete old draft snapshot.")
            self.meta_db.query(FlowSnapshot).filter_by(flow_id=db_flow.id, is_draft=True).delete()
        if not is_draft:
            self.meta_db.query(FlowSnapshot).filter(and_(
                FlowSnapshot.flow_id == db_flow.id,
                FlowSnapshot.is_current == True,
                FlowSnapshot.version != last_snap.version,
            )).update({"is_current": False})
            if last_snap and not last_snap.is_current:
                logger.info(f"▶️ Make current snapshot to {last_snap.version}.")
                last_snap.is_current = True
                last_snap.is_draft = False
                last_snap.op = op.name
                last_snap.message = last_snap.message + "\n" + message
                last_snap.payload = payload
                last_snap.normalized_payload = normalized_payload
                last_snap.payload_hash = payload_hash
                return last_snap, True
            last_snap.is_current = False

        new_version = self.next_version(db_flow.id)
        snap = FlowSnapshot(
            flow=db_flow,
            version=new_version,
            op=op.name,
            message=message,
            payload=payload,
            normalized_payload=normalized_payload,
            payload_hash=payload_hash,
            is_draft=is_draft,
            is_current=not is_draft,
        )
        logger.info(f"🆕 Create new snapshot to {snap.version}.")
        self.meta_db.add(snap)
        return snap, True

    def restore_flow_by_snapshot(self, flow_id: str, version: int):
        logger.info(f"▶️ Start to restore snapshot {version}.")
        fs = (self.meta_db.query(FlowSnapshot)
              .filter_by(flow_id=flow_id, version=version)
              .one())
        data = fs.payload

        flow = self._get_flow(flow_id)

        # 기존 task/edge 삭제 후 스냅샷으로 재구성
        flow.tasks.clear()
        flow.edges.clear()

        f = data["flow"]
        flow.name = f["name"]
        flow.is_draft = f["is_draft"]
        flow.dag_id = f["dag_id"]
        flow.description = f["description"]
        flow.owner_id = f["owner_id"]
        flow.hash = f["hash"]
        flow.file_hash = f["file_hash"]
        flow.is_loaded_by_airflow = f["is_loaded_by_airflow"]
        flow.schedule = f["schedule"]
        flow.schedule_options = f.get("schedule_options")
        flow.is_deleted = f["is_deleted"]
        flow.active_status = f["active_status"]
        flow.max_retries = f["max_retries"]

        # tasks 복원
        for t in data["tasks"]:
            task = DBTask(
                id=t["id"],
                variable_id=t["variable_id"],
                kind=t["kind"],
                code_string=t["code_string"],
                code_hash=t["code_hash"],
                python_libraries=t["python_libraries"],
                impl_namespace=t["impl_namespace"],
                impl_callable=t["impl_callable"],
                input_properties=t["input_properties"],
                output_properties=t["output_properties"],
                ui_type=t["ui_type"],
                ui_label=t["ui_label"],
                ui_position=t["ui_position"],
                ui_class=t["ui_class"],
                ui_style=t["ui_style"],
                ui_extra_data=t["ui_extra_data"],
            )
            task.inputs = [
                TaskInput(
                    id=inp.get("id"),
                    key=inp["key"],
                    type=inp["type"],
                    value=inp["value"],
                ) for inp in t["inputs"]
            ]
            flow.tasks.append(task)

        # edges 복원
        for e in data["edges"]:
            flow.edges.append(DBEdge(
                id=e["id"],
                from_task_id=e["from_task_id"],
                to_task_id=e["to_task_id"],
                ui_type=e["ui_type"],
                ui_label=e["ui_label"],
                ui_labelStyle=e["ui_label_style"],
                ui_labelBgStyle=e["ui_label_bg_style"],
                ui_labelBgPadding=e["ui_label_bg_padding"],
                ui_labelBgBorderRadius=e["ui_label_bg_border_radius"],
                ui_style=e["ui_style"],
            ))

        self.meta_db.flush()
        self.save_flow_snapshot(flow,
                                op=SnapshotOperation.RESTORE,
                                message=f"이전 버전 restore - v{version}",
                                is_draft=flow.is_draft, )
        self.meta_db.commit()
        logger.info(f"✅ Success to restore snapshot {version}.")
        return flow.id

    def create_dummy(self):
        logger.info(f"🆕 Create dummy flow")
        now_timestamp = datetime.datetime.now(datetime.timezone.utc)

        name = "Workflow_" + now_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        dummy_flow = DBFlow(
            name=name,
            dag_id=make_flow_id_by_name(name),
            is_draft=True,
        )
        self.meta_db.add(dummy_flow)
        self.meta_db.flush()
        _, is_snap_changed = self.save_flow_snapshot(dummy_flow,
                                                     SnapshotOperation.DUMMY,
                                                     message=Config.DUMMY_MSG,
                                                     is_draft=True,
                                                     )
        self.meta_db.commit()
        return flow_db2domain(dummy_flow)

    def save_dag(self, dag: DAGRequest):
        existing = self.find_existing_flow(dag.name)

        if existing:
            raise WorkflowError("DAG already exists")
        else:
            logger.info(f"🆕 Create New DAG: {dag.name}")
            domain_flow = flow_api2domain(dag)
            db_flow = flow_domain2db(domain_flow, self.airflow_db)
            self.meta_db.add(db_flow)
            self.meta_db.flush()
            _, is_snap_changed = self.save_flow_snapshot(db_flow,
                                                         SnapshotOperation.CREATE,
                                                         message="신규 등록",
                                                         is_draft=dag.is_draft,
                                                         )
            if is_snap_changed:
                db_flow.file_hash = domain_flow.file_hash
            self.meta_db.commit()
            return flow_db2domain(db_flow)

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
            snap, is_snap_changed = self.save_flow_snapshot(
                origin_flow,
                SnapshotOperation.UPDATE,
                message="draft=True",
                is_draft=new_dag.is_draft,
                payload=build_flow_snapshot_by_domain(new_flow, origin_dag_id)
            )
            if is_snap_changed:
                origin_flow.is_draft = new_flow.is_draft
                origin_flow.description = new_flow.description
                origin_flow.owner_id = new_flow.owner_id
                origin_flow.schedule = new_flow.scheduled
                origin_flow.schedule_options = new_flow.schedule_options
            else:
                if snap.op == SnapshotOperation.DUMMY.name:
                    if not snap.payload.get("tasks", []):
                        logger.warning("🧹 Delete unchanged Dummy Flow")
                        self.meta_db.delete(origin_flow)
            self.meta_db.commit()
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
            origin_flow.name = new_flow.name
            origin_flow.dag_id = new_flow.dag_id

        # 3. 필드 갱신
        origin_flow.description = new_flow.description
        origin_flow.owner_id = new_flow.owner_id
        origin_flow.schedule = new_flow.scheduled
        origin_flow.schedule_options = new_flow.schedule_options
        origin_flow.hash = hash(new_flow)
        origin_flow.active_status = new_flow.active_status
        origin_flow.max_retries = new_flow.max_retries

        origin_flow.tasks.clear()
        origin_flow.edges.clear()

        origin_flow.tasks, origin_flow.edges = task_edge_domain2db(origin_flow, new_flow.tasks, new_flow.edges)
        self.meta_db.flush()

        try:
            snap, is_snap_changed = self.save_flow_snapshot(origin_flow,
                                                            SnapshotOperation.UPDATE,
                                                            message="필드 수정",
                                                            is_draft=new_dag.is_draft,
                                                            )
            origin_flow.is_draft = new_flow.is_draft
            if is_snap_changed:
                origin_flow.file_hash = new_flow.file_hash
            self.meta_db.commit()
            return flow_db2domain(origin_flow)
        except Exception as e:
            self.meta_db.rollback()
            raise WorkflowError(f"❌ DAG 업데이트 실패: {e}")

    def update_dag_active_status(self, dag_id: str, active_status: bool) -> bool:
        flow = self._get_flow(dag_id)
        try:
            result = self.airflow_client.update_pause(flow.dag_id, False if active_status else True)
            logger.info(f"🔄 Update airflow is_paused to '{result}'")
            flow.active_status = active_status
            self.save_flow_snapshot(flow, SnapshotOperation.UPDATE, message="activate status 수정")
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
            .filter(FS.is_current.is_(True))
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
        if execution_status:
            query = query.filter(FEQ2.status.in_(execution_status))
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
                             .with_entities(DBFlow, )
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
        filtered_count = query.count()

        # limit 적용
        query = query.offset(offset).limit(limit)

        # get list
        flows = query.all()
        result_count = len(flows)

        return [flow_db2domain(dbflow, execution_status) for dbflow, execution_status in flows], result_count, filtered_count, total_count

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
        current_flow = query.filter(FlowSnapshot.is_current == True).first()
        draft_flow = query.filter(FlowSnapshot.is_draft == True).first()
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
        self.save_flow_snapshot(flow, SnapshotOperation.DELETE, message="임시 삭제")
        self.meta_db.commit()

        delete_dag_file(flow)
        logger.info(f"🧹 Complete to delete dag temporary: {flow.name}")
        return dag_id

    def delete_dag_permanently(self, dag_id: str):
        flow = self._get_flow(dag_id)

        self.save_flow_snapshot(flow, SnapshotOperation.DELETE, message="완전 삭제")
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
        domain_flow = flow_db2domain(flow)
        flow.file_hash = domain_flow.file_hash

        self.save_flow_snapshot(flow, SnapshotOperation.RESTORE, message="임시 삭제 flow 복구")
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
