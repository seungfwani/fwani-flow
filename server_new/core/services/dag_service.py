import datetime

from sqlalchemy.orm import Session

from models.api.dag_model import DAGRequest
from models.db.airflow_mapper import AirflowDag
from models.db.flow_model import FlowModel
from models.db.task import Task as DBTask
from models.domain.flow import Flow as DomainFlow, Task as DomainTask


class DagService:
    def __init__(self, dag: DAGRequest, domain_db: Session, airflow_db: Session):
        self.dag = dag
        self.domain_db = domain_db
        self.airflow_db = airflow_db

    def convert_dag(self):
        return DomainFlow(
            self.dag.name,
            self.dag.description,
            self.dag.owner,
            self.dag.schedule,
            self.dag.nodes,
            self.dag.edges,
        )

    def save_dag(self):
        flow = self.convert_dag()
        db_flow = domain_flow_to_db_flow(flow, self.airflow_db)
        self.domain_db.add(db_flow)
        self.domain_db.commit()


def domain_task_to_db_task(domain_task: DomainTask):
    return DBTask(
        id=domain_task.id,
        variable_id=domain_task.variable_id,
        python_libraries=domain_task.python_libraries,
        code_string=domain_task.code,
        code=domain_task.code_hash,
        ui_type=domain_task.ui_type,
        ui_label=domain_task.ui_label,
        ui_position=domain_task.ui_position,
        ui_style=domain_task.ui_style,
    )


def domain_flow_to_db_flow(domain_flow: DomainFlow, airflow_db: Session):
    return FlowModel(
        id=domain_flow.id,
        name=domain_flow.name,
        description=domain_flow.description,
        owner_id=domain_flow.owner,
        hash=hash(domain_flow),
        file_hash=domain_flow.file_hash,
        scheduled=domain_flow.scheduled,
        is_loaded_by_airflow=check_loaded_by_airflow(domain_flow.write_time, domain_flow.id, airflow_db),
    )


def get_last_parsed_time_of_airflow_dag(dag_id: str, airflow_db: Session):
    airflow_dag = airflow_db.query(AirflowDag).filter(AirflowDag.dag_id == dag_id).first()
    if airflow_dag is None:
        return None
    return airflow_dag.last_parsed_time


def check_loaded_by_airflow(write_file_time: datetime.datetime, dag_id: str, airflow_db: Session):
    last_parsed_time = get_last_parsed_time_of_airflow_dag(dag_id, airflow_db)
    if last_parsed_time:
        return last_parsed_time > write_file_time
    else:
        return False
