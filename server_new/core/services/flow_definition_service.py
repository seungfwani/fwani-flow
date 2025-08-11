import logging
import os
import shutil

from sqlalchemy import or_, and_, func, asc, desc
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.operators import like_op

from config import Config
from core.airflow_client import AirflowClient
from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.flow import Flow as DBFlow
from models.db.flow_execution_queue import FlowExecutionQueue
from models.domain.mapper import flow_api2domain, flow_db2domain, flow_domain2db, task_edge_domain2db, flow_domain2api

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
            .filter(and_(DBFlow.name == dag_name,
                         DBFlow.is_draft == False))
            .first()
        )

    def check_available_dag_name(self, dag_name: str) -> bool:
        flow = self.find_existing_flow(dag_name=dag_name)
        if flow:
            return False
        else:
            return True

    def save_dag(self, dag: DAGRequest):
        existing = self.find_existing_flow(dag.name)

        if existing:
            raise WorkflowError("DAG already exists")
        else:
            logger.info(f"🆕 DAG 신규 등록: {dag.name}")
            domain_flow = flow_api2domain(dag)
            db_flow = flow_domain2db(domain_flow, self.airflow_db)
            self.meta_db.add(db_flow)
            self.meta_db.commit()
        return db_flow.id

    def update_dag(self, origin_dag_id: str, new_dag: DAGRequest):
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
        origin_flow.description = new_flow.description
        origin_flow.schedule = new_flow.scheduled
        origin_flow.hash = hash(new_flow)
        origin_flow.file_hash = new_flow.file_hash

        origin_flow.tasks.clear()
        origin_flow.edges.clear()

        origin_flow.tasks, origin_flow.edges = task_edge_domain2db(origin_flow, new_flow.edges)

        try:
            self.meta_db.commit()
            return origin_flow.id
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
        self.meta_db.commit()
        return active_status

    def get_dag_total_count(self):
        return self.meta_db.query(func.count(DBFlow.id)).scalar()

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
        total_count = query.count()

        # limit 적용
        query = query.offset(offset).limit(limit)

        # get list
        flows = query.all()
        filtered_count = len(flows)

        return [flow_domain2api(flow_db2domain(dbflow)) for dbflow in flows], filtered_count, total_count

    def get_dag(self, dag_id):
        flow = self._get_flow(dag_id)
        return flow_domain2api(flow_db2domain(flow))

    def delete_dag_temporary(self, dag_id: str):
        flow = self._get_flow(dag_id)

        delete_dag_file(flow.dag_id)

        flow.is_deleted = True
        flow.file_hash = None
        self.meta_db.commit()
        logger.info(f"🕒 DAG 임시 삭제됨 (DB 보관): {flow.name}")
        return dag_id

    def delete_dag_permanently(self, dag_id: str):
        flow = self._get_flow(dag_id)

        delete_dag_file(flow.dag_id)

        self.meta_db.delete(flow)
        self.meta_db.commit()
        logger.info(f"💥 DAG 영구 삭제됨: {flow.name}")
        return dag_id

    def restore_deleted_dag(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow or not flow.is_deleted:
            raise WorkflowError(f"Flow({dag_id}) not found")

        flow.is_deleted = False
        flow.file_hash = flow_db2domain(flow).file_hash

        self.meta_db.commit()
        logger.info(f"♻️ DAG 복구됨: {flow.name}")


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
