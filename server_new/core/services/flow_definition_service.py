import logging

from sqlalchemy.orm import Session

from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.flow import Flow as DBFlow
from models.domain.models import delete_dag_file
from models.domain.mapper import flow_api2domain, flow_db2domain, flow_domain2db, task_edge_domain2db, flow_domain2api

logger = logging.getLogger()


class FlowDefinitionService:
    def __init__(self, meta_db: Session, airflow_db: Session):
        self.meta_db = meta_db
        self.airflow_db = airflow_db

    def find_existing_flow(self, dag: DAGRequest):
        return (
            self.meta_db.query(DBFlow)
            # .filter((DBFlow.id == self.dag.id) | (DBFlow.name == self.dag.name))
            .filter(DBFlow.name == dag.name)
            .first()
        )

    def save_dag(self, dag: DAGRequest):
        existing = self.find_existing_flow(dag)

        if existing:
            raise WorkflowError("DAG already exists")
        else:
            logger.info(f"🆕 DAG 신규 등록: {dag.name}")
            domain_flow = flow_api2domain(dag)
            db_flow = flow_domain2db(domain_flow, self.airflow_db)
            self.meta_db.add(db_flow)
            self.meta_db.commit()

    def update_dag(self, origin_dag_id: str, new_dag: DAGRequest):
        # 0. 기존 Flow 조회
        origin_flow = (
            self.meta_db.query(DBFlow)
            .filter(DBFlow.id == origin_dag_id)
            .first()
        )
        if origin_flow is None:
            raise WorkflowError(f"DAG ({origin_dag_id}) 가 존재하지 않습니다.")

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
        except Exception as e:
            self.meta_db.rollback()
            msg = f"❌ DAG 업데이트 실패: {e}"
            logger.error(msg)
            raise WorkflowError(msg)

    def get_active_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).filter(DBFlow.is_deleted == False).all()

    def get_all_flows(self) -> list[DBFlow]:
        return self.meta_db.query(DBFlow).all()

    def get_dag_list(self, is_deleted=False):
        if is_deleted:
            flows = self.get_all_flows()
        else:
            flows = self.get_active_flows()
        return [flow_domain2api(flow_db2domain(dbflow)) for dbflow in flows]

    def get_dag(self, dag_id):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if flow is None:
            raise WorkflowError(f"DAG ({dag_id}) 가 존재하지 않습니다.")
        return flow_domain2api(flow_db2domain(flow))

    def delete_dag_temporary(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow:
            raise WorkflowError(f"Flow({dag_id}) not found")

        delete_dag_file(flow.dag_id)

        flow.is_deleted = True
        flow.file_hash = None
        self.meta_db.commit()
        logger.info(f"🕒 DAG 임시 삭제됨 (DB 보관): {flow.name}")

    def delete_dag_permanently(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow:
            raise WorkflowError(f"Flow({dag_id}) not found")

        delete_dag_file(flow.dag_id)

        self.meta_db.delete(flow)
        self.meta_db.commit()
        logger.info(f"💥 DAG 영구 삭제됨: {flow.name}")

    def restore_deleted_dag(self, dag_id: str):
        flow = self.meta_db.query(DBFlow).filter(DBFlow.id == dag_id).first()
        if not flow or not flow.is_deleted:
            raise WorkflowError(f"Flow({dag_id}) not found")

        flow.is_deleted = False
        flow.file_hash = flow_db2domain(flow).file_hash

        self.meta_db.commit()
        logger.info(f"♻️ DAG 복구됨: {flow.name}")
