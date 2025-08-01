import json
import uuid

from sqlalchemy import Column, String, DateTime, Boolean, func, ForeignKey, JSON, Numeric, BIGINT, Integer, Float
from sqlalchemy.orm import relationship

from core.database import BaseDB, AirflowDB
from utils.functions import string2datetime

class AirflowDag(AirflowDB):
    __tablename__ = "dag"

    dag_id = Column(String, primary_key=True)
    is_paused = Column(Boolean)
    last_parsed_time = Column(DateTime)


class AirflowDagCode(AirflowDB):
    __tablename__ = "dag_code"

    fileloc_hash = Column(BIGINT, primary_key=True)
    fileloc = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    source_code = Column(String, nullable=False)


class AirflowTaskInstance(AirflowDB):
    __tablename__ = "task_instance"

    task_id = Column(String, primary_key=True)
    dag_id = Column(String, primary_key=True)
    run_id = Column(String, primary_key=True)
    map_index = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Float)
    state = Column(String)
    try_number = Column(Integer)
    updated_at = Column(DateTime)

    # def __eq__(self, other):
    #     if not isinstance(other, AirflowDagRunHistory):
    #         return False
    #     return (
    #             self.flow_version_id == other.flow_version_id and
    #             self.run_id == other.run_id and
    #             self.status == other.status
    #     )
    #
    # def __hash__(self):
    #     return hash((self.flow_version_id, self.run_id, self.status))
    #
    # @classmethod
    # def from_json(cls, flow_version: FlowVersion, data: dict) -> "AirflowDagRunHistory":
    #     dag_run_history = AirflowDagRunHistory(
    #         id=str(uuid.uuid4()),  # 트리거 API 로 요청된 것과 id 를 맞추기 위함 ??
    #         dag_id=data["dag_id"],
    #         run_id=data["dag_run_id"],
    #         execution_date=string2datetime(data.get("execution_date")),
    #         start_date=string2datetime(data.get("start_date")),
    #         end_date=string2datetime(data.get("end_date")),
    #         status=data.get("state"),
    #         external_trigger=data.get("external_trigger"),
    #         run_type=data.get("run_type"),
    #         conf=json.dumps(data.get("conf")),
    #         source=data.get("conf", {}).get("source", "airflow"),
    #         flow_version=flow_version,
    #     )
    #     for task in flow_version.tasks:
    #         dag_run_history.snapshot_tasks.append(
    #             AirflowDagRunSnapshotTask(
    #                 task_id=task.id,
    #                 variable_id=task.variable_id,
    #                 function_id=task.function_id,
    #                 inputs={inp.key: inp.value for inp in task.inputs},
    #                 type=task.task_ui.type,
    #                 label=task.function.name,
    #                 position=task.task_ui.position,
    #                 style=task.task_ui.style,
    #             )
    #         )
    #     for edge in flow_version.edges:
    #         dag_run_history.snapshot_edges.append(
    #             AirflowDagRunSnapshotEdge(
    #                 edge_id=edge.id,
    #                 source=edge.from_task_id,
    #                 target=edge.to_task_id,
    #                 type=edge.edge_ui.type,
    #                 label=edge.edge_ui.label,
    #                 labelStyle=edge.edge_ui.labelStyle,
    #                 labelBgStyle=edge.edge_ui.labelBgStyle,
    #                 labelBgPadding=edge.edge_ui.labelBgPadding,
    #                 labelBgBorderRadius=edge.edge_ui.labelBgBorderRadius,
    #                 style=edge.edge_ui.style,
    #             )
    #         )
#         return dag_run_history
#
#
# class AirflowDagRunSnapshotTask(BaseDB):
#     __tablename__ = "airflow_dag_run_snapshot_task"
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     dag_run_history_id = Column(String, ForeignKey("airflow_dag_run_history.id", ondelete="CASCADE"), nullable=False)
#     task_id = Column(String)
#     variable_id = Column(String)
#     function_id = Column(String)
#     inputs = Column(JSON)
#     # ui 관련
#     type = Column(String)
#     label = Column(String)
#     position = Column(JSON)
#     style = Column(JSON)
#
#     dag_run_history = relationship("AirflowDagRunHistory", back_populates="snapshot_tasks")
#
#
# class AirflowDagRunSnapshotEdge(BaseDB):
#     __tablename__ = "airflow_dag_run_snapshot_edge"
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     dag_run_history_id = Column(String, ForeignKey("airflow_dag_run_history.id", ondelete="CASCADE"), nullable=False)
#     edge_id = Column(String)
#     source = Column(String)
#     target = Column(String)
#     type = Column(String)
#     label = Column(String)
#     labelStyle = Column(JSON)
#     labelBgStyle = Column(JSON)
#     labelBgPadding = Column(JSON)
#     labelBgBorderRadius = Column(Numeric)
#     style = Column(JSON)
#
#     dag_run_history = relationship("AirflowDagRunHistory", back_populates="snapshot_edges")
