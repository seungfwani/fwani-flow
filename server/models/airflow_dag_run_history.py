import json
import uuid

from sqlalchemy import Column, String, DateTime, Boolean, func, ForeignKey, JSON, Numeric
from sqlalchemy.orm import relationship

from core.database import Base
from models.flow_version import FlowVersion
from utils.functions import string2datetime


class AirflowDagRunHistory(Base):
    __tablename__ = "airflow_dag_run_history"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    flow_version_id = Column(String, ForeignKey("flow_version.id", ondelete="CASCADE"), nullable=False)
    dag_id = Column(String, nullable=False)
    run_id = Column(String, nullable=False)
    execution_date = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(String)  # e.g., 'success', 'failed'
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String)  # e.g., 'manual', 'scheduled'
    conf = Column(String)
    source = Column(String, nullable=True)  # e.g. 'ui', 'api', 'cli'
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now())

    flow_version = relationship("FlowVersion", back_populates="airflow_dag_run_histories")
    snapshot_tasks = relationship("AirflowDagRunSnapshotTask", back_populates="dag_run_history",
                                  cascade="all, delete-orphan")
    snapshot_edges = relationship("AirflowDagRunSnapshotEdge", back_populates="dag_run_history",
                                  cascade="all, delete-orphan")

    @classmethod
    def from_json(cls, flow_version: FlowVersion, data: dict) -> "AirflowDagRunHistory":
        dag_run_history = AirflowDagRunHistory(
            id=str(uuid.uuid4()),  # 트리거 API 로 요청된 것과 id 를 맞추기 위함 ??
            dag_id=data["dag_id"],
            run_id=data["dag_run_id"],
            execution_date=string2datetime(data.get("execution_date")),
            start_date=string2datetime(data.get("start_date")),
            end_date=string2datetime(data.get("end_date")),
            status=data.get("state"),
            external_trigger=data.get("external_trigger"),
            run_type=data.get("run_type"),
            conf=json.dumps(data.get("conf")),
            source=data.get("conf", {}).get("source", "airflow"),
            flow_version=flow_version,
        )
        for task in flow_version.tasks:
            dag_run_history.snapshot_tasks.append(
                AirflowDagRunSnapshotTask(
                    task_id=task.id,
                    function_id=task.function_id,
                    inputs={inp.key: inp.value for inp in task.inputs},
                    type=task.task_ui.type,
                    label=task.function.name,
                    position=task.task_ui.position,
                    style=task.task_ui.style,
                )
            )
        for edge in flow_version.edges:
            dag_run_history.snapshot_edges.append(
                AirflowDagRunSnapshotEdge(
                    edge_id=edge.id,
                    source=edge.source,
                    target=edge.target,
                    type=edge.edge_ui.type,
                    label=edge.edge_ui.label,
                    labelStyle=edge.edge_ui.labelStyle,
                    labelBgStyle=edge.edge_ui.labelBgStyle,
                    labelBgPadding=edge.edge_ui.labelBgPadding,
                    labelBgBorderRadius=edge.edge_ui.labelBgBorderRadius,
                    style=edge.edge_ui.style,
                )
            )


class AirflowDagRunSnapshotTask(Base):
    __tablename__ = "airflow_dag_run_snapshot_task"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dag_run_history_id = Column(String, ForeignKey("airflow_dag_run_history.id", ondelete="CASCADE"), nullable=False)
    task_id = Column(String)
    function_id = Column(String)
    inputs = Column(JSON)
    # ui 관련
    type = Column(String)
    label = Column(String)
    position = Column(JSON)
    style = Column(JSON)

    dag_run_history = relationship("AirflowDagRunHistory", back_populates="snapshot_tasks")


class AirflowDagRunSnapshotEdge(Base):
    __tablename__ = "airflow_dag_run_snapshot_edge"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dag_run_history_id = Column(String, ForeignKey("airflow_dag_run_history.id", ondelete="CASCADE"), nullable=False)
    edge_id = Column(String)
    source = Column(String)
    target = Column(String)
    type = Column(String)
    label = Column(String)
    labelStyle = Column(JSON)
    labelBgStyle = Column(JSON)
    labelBgPadding = Column(JSON)
    labelBgBorderRadius = Column(Numeric)
    style = Column(JSON)

    dag_run_history = relationship("AirflowDagRunHistory", back_populates="snapshot_edges")
