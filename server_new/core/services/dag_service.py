import datetime
import logging
from typing import List

from sqlalchemy.orm import Session

from errors import WorkflowError
from models.api.dag_model import DAGRequest
from models.db.airflow_mapper import AirflowDag
from models.db.edge import Edge as DBEdge
from models.db.flow import Flow as DBFlow
from models.db.task import Task as DBTask
from models.domain.flow import Flow as DomainFlow, Task as DomainTask, Edge as DomainEdge

logger = logging.getLogger()


class DagService:
    def __init__(self, domain_db: Session, airflow_db: Session):
        self.meta_db = domain_db
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
            domain_flow = DomainFlow.from_dag_request(dag)
            db_flow = domain_flow_to_db_flow(domain_flow, self.airflow_db)
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
        new_flow = DomainFlow.from_dag_request(new_dag)
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

        origin_flow.tasks, origin_flow.edges = domain_task_edge_to_db_task_edge(origin_flow, new_flow.edges)

        try:
            self.meta_db.commit()
        except Exception as e:
            self.meta_db.rollback()
            msg = f"❌ DAG 업데이트 실패: {e}"
            logger.error(msg)
            raise WorkflowError(msg)

    def get_dag_list(self):
        return [DomainFlow.from_db_flow(dbflow) for dbflow in self.meta_db.query(DBFlow).all()]


def domain_task_edge_to_db_task_edge(flow: DBFlow, domain_edges: List[DomainEdge]):
    def domain_task_to_db_task(flow_: DBFlow, domain_task: DomainTask):
        return DBTask(
            flow=flow_,
            id=domain_task.id,
            variable_id=domain_task.variable_id,
            python_libraries=domain_task.python_libraries,
            code_string=domain_task.code,
            code_hash=domain_task.code_hash,
            ui_type=domain_task.ui_type,
            ui_label=domain_task.ui_label,
            ui_position=domain_task.ui_position,
            ui_style=domain_task.ui_style,
        )

    tasks_cache: dict[str, DBTask] = {}
    edges: list[DBEdge] = []

    # 모든 edge 반복하며 task + edge 생성
    for domain_edge in domain_edges:
        source_key = domain_edge.source.id
        target_key = domain_edge.target.id

        if source_key not in tasks_cache:
            tasks_cache[source_key] = domain_task_to_db_task(flow, domain_edge.source)
        if target_key not in tasks_cache:
            tasks_cache[target_key] = domain_task_to_db_task(flow, domain_edge.target)

        source_task = tasks_cache[source_key]
        target_task = tasks_cache[target_key]

        db_edge = DBEdge(
            flow=flow,
            from_task=source_task,
            to_task=target_task,
            ui_type=domain_edge.ui_type,
            ui_label=domain_edge.ui_label,
            ui_labelStyle=domain_edge.ui_label_style,
            ui_labelBgPadding=domain_edge.ui_label_bg_padding,
            ui_labelBgBorderRadius=domain_edge.ui_label_bg_border_radius,
            ui_style=domain_edge.ui_style,
        )
        edges.append(db_edge)
    return list(tasks_cache.values()), edges


def domain_flow_to_db_flow(domain_flow: DomainFlow, airflow_db: Session):
    flow = DBFlow(
        name=domain_flow.name,
        dag_id=domain_flow.dag_id,
        description=domain_flow.description,
        owner_id=domain_flow.owner,
        hash=hash(domain_flow),
        file_hash=domain_flow.file_hash,
        schedule=domain_flow.scheduled,
        is_loaded_by_airflow=check_loaded_by_airflow(domain_flow.write_time, domain_flow.dag_id, airflow_db),
    )
    # 관계 설정
    flow.tasks, flow.edges = domain_task_edge_to_db_task_edge(flow, domain_flow.edges)
    return flow


def check_loaded_by_airflow(write_file_time: datetime.datetime, dag_id: str, airflow_db: Session):
    airflow_dag = airflow_db.query(AirflowDag).filter(AirflowDag.dag_id == dag_id).first()
    if airflow_dag is None:
        return False
    if airflow_dag.last_parsed_time:
        return airflow_dag.last_parsed_time > write_file_time
    else:
        return False
