import datetime
import logging
import os
import shutil

from sqlalchemy import or_, and_, func, asc, desc
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.operators import like_op

from config import Config
from core.airflow_client import AirflowClient
from core.snapshot import build_flow_snapshot, SnapshotOperation
from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.edge import Edge as DBEdge
from models.db.flow import Flow as DBFlow, FlowSnapshot
from models.db.flow_execution_queue import FlowExecutionQueue
from models.db.task import Task as DBTask, TaskInput
from models.domain.mapper import flow_api2domain, flow_db2domain, flow_domain2db, task_edge_domain2db, flow_snapshot2api

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
                           flow: DBFlow,
                           op: SnapshotOperation,
                           message: str | None = None,
                           is_draft: bool = False,
                           upsert_draft: bool = True,
                           ):
        # 스냅샷 페이로드 & 해시 생성
        payload, payload_hash = build_flow_snapshot(flow)

        # 변경점 확인 최적화
        last_snap = (self.meta_db.query(FlowSnapshot)
                     .filter(FlowSnapshot.flow_id == flow.id)
                     .order_by(desc(FlowSnapshot.version))
                     .first())
        if last_snap and last_snap.payload_hash == payload_hash:
            return last_snap, False

        # draft/current 정리
        if is_draft and upsert_draft:
            self.meta_db.query(FlowSnapshot).filter_by(flow_id=flow.id, is_draft=True).delete()
        if not is_draft:
            if last_snap and not last_snap.is_current:
                last_snap.is_current = True
                last_snap.is_draft = False
                last_snap.op = op.name
                last_snap.message = last_snap.message + "\n" + message
                last_snap.payload = payload
                last_snap.payload_hash = payload_hash
                return last_snap, True
            self.meta_db.query(FlowSnapshot).filter_by(flow_id=flow.id, is_current=True).update({"is_current": False})

        new_version = self.next_version(flow.id)
        snap = FlowSnapshot(
            flow=flow,
            version=new_version,
            op=op.name,
            message=message,
            payload=payload,
            payload_hash=payload_hash,
            is_draft=is_draft,
            is_current=not is_draft,
        )
        self.meta_db.add(snap)
        return snap, True

    def restore_flow_by_snapshot(self, flow_id: str, version: int):
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
                input_meta_type=t["input_meta_type"],
                output_meta_type=t["output_meta_type"],
                ui_type=t["ui_type"],
                ui_label=t["ui_label"],
                ui_position=t["ui_position"],
                ui_style=t["ui_style"],
                ui_extra_data=t["ui_extra_data"],
            )
            task.inputs = [
                TaskInput(
                    id=inp["id"],
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
                ui_labelStyle=e["ui_labelStyle"],
                ui_labelBgStyle=e["ui_labelBgStyle"],
                ui_labelBgPadding=e["ui_labelBgPadding"],
                ui_labelBgBorderRadius=e["ui_labelBgBorderRadius"],
                ui_style=e["ui_style"],
            ))

        self.meta_db.flush()
        self.save_flow_snapshot(flow,
                                op=SnapshotOperation.RESTORE,
                                message=f"이전 버전 restore - v{version}",
                                is_draft=flow.is_draft, )
        self.meta_db.commit()
        return flow.id

    def create_dummy(self):
        now_timestamp = datetime.datetime.now(datetime.timezone.utc)

        dummy_flow = DBFlow(
            name="Workflow_" + now_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
            is_draft=True,
        )
        self.meta_db.add(dummy_flow)
        self.meta_db.commit()
        return flow_db2domain(dummy_flow)

    def save_dag(self, dag: DAGRequest):
        existing = self.find_existing_flow(dag.name)

        if existing:
            raise WorkflowError("DAG already exists")
        else:
            logger.info(f"🆕 DAG 신규 등록: {dag.name}")
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
            return origin_dag_id
        # 0. 기존 Flow 조회
        origin_flow = self._get_flow(origin_dag_id)

        # 1. 변환
        new_flow = flow_api2domain(new_dag)
        # 이름이 바뀐 경우 → 중복 확인
        if new_flow.name != origin_flow.name:
            duplicate = (
                self.meta_db.query(DBFlow)
                .filter(DBFlow.name == new_flow.name, DBFlow.id != origin_flow.id)
                .first()
            )
            if duplicate:
                raise WorkflowError(f"Flow 이름 '{new_flow.name}' 은 이미 존재합니다.")
        # 3. 필드 갱신
        origin_flow.name = new_flow.name
        origin_flow.dag_id = new_flow.dag_id
        origin_flow.description = new_flow.description
        origin_flow.schedule = new_flow.scheduled
        origin_flow.hash = hash(new_flow)
        origin_flow.active_status = new_flow.active_status
        origin_flow.max_retries = new_flow.max_retries
        origin_flow.is_draft = new_flow.is_draft

        origin_flow.tasks.clear()
        origin_flow.edges.clear()

        origin_flow.tasks, origin_flow.edges = task_edge_domain2db(origin_flow, new_flow.edges)
        self.meta_db.flush()

        try:
            _, is_snap_changed = self.save_flow_snapshot(origin_flow,
                                                         SnapshotOperation.UPDATE,
                                                         message="필드 수정",
                                                         is_draft=new_dag.is_draft,
                                                         )
            if is_snap_changed:
                origin_flow.file_hash = new_flow.file_hash
            self.meta_db.commit()
            return flow_db2domain(origin_flow)
        except Exception as e:
            self.meta_db.rollback()
            msg = f"❌ DAG 업데이트 실패: {e}"
            logger.error(msg)
            raise WorkflowError(msg)

    def update_dag_active_status(self, dag_id: str, active_status: bool) -> bool:
        flow = self._get_flow(dag_id)
        result = self.airflow_client.update_pause(flow.dag_id, False if active_status else True)
        logger.info(f"Update airflow is_paused to '{result}'")
        flow.active_status = active_status
        self.save_flow_snapshot(flow, SnapshotOperation.UPDATE, message="activate status 수정")
        self.meta_db.commit()
        return active_status

    def get_dag_total_count(self):
        return self.meta_db.query(func.count(DBFlow.id)).filter(DBFlow.is_deleted == False).scalar()

    def get_active_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).filter(DBFlow.is_deleted == False).all()

    def get_all_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).all()

    def get_dag_list(self,
                     active_status: set[bool],
                     execution_status: set[str],
                     name: str,
                     sort: str,
                     offset: int = 0,
                     limit: int = 10,
                     include_deleted=False):
        query = self.meta_db.query(DBFlow)
        if execution_status:
            FEQ1 = aliased(FlowExecutionQueue)
            FEQ2 = aliased(FlowExecutionQueue)
            subquery = (self.meta_db
                        .query(FEQ1.flow_id.label("flow_id"),
                               func.max(FEQ1.updated_at).label("updated_at"))
                        .group_by(FEQ1.flow_id)
                        .subquery())
            query = (query.outerjoin(subquery, DBFlow.id == subquery.c.flow_id)
                     .outerjoin(FEQ2, and_(FEQ2.flow_id == subquery.c.flow_id,
                                           FEQ2.updated_at == subquery.c.updated_at)))
            filters = []
            for es in execution_status:
                filters.append(FEQ2.status == es)
            query = query.filter(or_(*filters))
        if active_status:
            query = query.filter(or_(*[DBFlow.active_status == i for i in active_status]))
        if name:
            query = query.filter(like_op(DBFlow.name, f"%{name}%"))
        if sort:
            field, direction = sort.split("_")
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

        return [flow_db2domain(dbflow) for dbflow in flows], result_count, filtered_count, total_count

    def get_dag(self, dag_id):
        query = (self.meta_db.query(FlowSnapshot)
                 .filter(FlowSnapshot.flow_id == dag_id))
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

        delete_dag_file(flow.dag_id)
        logger.info(f"🕒 DAG 임시 삭제됨 (DB 보관): {flow.name}")
        return dag_id

    def delete_dag_permanently(self, dag_id: str):
        flow = self._get_flow(dag_id)

        self.save_flow_snapshot(flow, SnapshotOperation.DELETE, message="완전 삭제")
        self.meta_db.delete(flow)
        self.meta_db.commit()

        delete_dag_file(flow.dag_id)
        logger.info(f"💥 DAG 영구 삭제됨: {flow.name}")
        return dag_id

    def restore_deleted_dag(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow or not flow.is_deleted:
            raise WorkflowError(f"Flow({dag_id}) not found")

        flow.is_deleted = False
        flow.file_hash = flow_db2domain(flow).file_hash

        self.save_flow_snapshot(flow, SnapshotOperation.RESTORE, message="임시 삭제 flow 복구")
        self.meta_db.commit()
        logger.info(f"♻️ DAG 복구됨: {flow.name}")
        return flow.id


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
