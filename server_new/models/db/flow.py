import uuid

from sqlalchemy import Column, String, Text, DateTime, func, Boolean, Integer, ForeignKey, UniqueConstraint, JSON, \
    Index, text, CheckConstraint
from sqlalchemy.orm import relationship, validates

from core.database import BaseDB
from utils.functions import make_flow_id_by_name


class Flow(BaseDB):
    __tablename__ = "flow"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True)
    is_draft = Column(Boolean, default=False)
    dag_id = Column(String, unique=True, index=True)
    description = Column(Text)
    owner_id = Column(String)
    hash = Column(String)  # node + edge
    file_hash = Column(String)
    is_loaded_by_airflow = Column(Boolean, default=False)
    schedule = Column(String, default=None)
    is_deleted = Column(Boolean, default=False)
    active_status = Column(Boolean, default=False)
    max_retries = Column(Integer, default=0)

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    tasks = relationship("Task", back_populates="flow", cascade="all, delete-orphan", passive_deletes=True)
    edges = relationship("Edge", back_populates="flow", cascade="all, delete-orphan", passive_deletes=True)
    flow_snapshots = relationship("FlowSnapshot",
                                  back_populates="flow",
                                  cascade="all, delete-orphan",
                                  passive_deletes=True,
                                  order_by="desc(FlowSnapshot.version)",
                                  )

    flow_execution_queues = relationship("FlowExecutionQueue",
                                         back_populates="flow",
                                         cascade="all, delete-orphan",
                                         passive_deletes=True,
                                         order_by="desc(FlowExecutionQueue.updated_at)", )
    airflow_dag_run_histories = relationship("AirflowDagRunHistory", back_populates="flow",
                                             cascade="all, delete-orphan",
                                             passive_deletes=True)

    def __repr__(self):
        return (f"<Flow ("
                f"  id={self.id}"
                f"  name={self.name}"
                f"  description={self.description}"
                f")>")

    def __eq__(self, other):
        if not isinstance(other, Flow):
            return False
        return all([
            self.name == other.name,
            self.description == other.description,
            self.owner_id == other.owner_id,
            self.schedule == other.schedule,
            self.tasks == other.tasks,
            self.edges == other.edges,
        ])

    def __hash__(self):
        return hash((
            self.name,
            self.description,
            self.owner_id,
            self.schedule,
            tuple(self.tasks),
            tuple(self.edges),
        ))

    @validates("name")
    def _update_dag_id(self, key, name):
        self.dag_id = make_flow_id_by_name(name)
        return name


class FlowSnapshot(BaseDB):
    __tablename__ = "flow_snapshot"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), index=True, nullable=False)
    version = Column(Integer, nullable=False)  # unique with flow_id

    # 현재/수정 버전 플래그
    is_current = Column(Boolean, nullable=False, server_default="false")
    is_draft = Column(Boolean, nullable=False, server_default="false")

    op = Column(String, nullable=False)  # create/update/delete/publish/restore
    message = Column(String, nullable=True)
    payload = Column(JSON, nullable=False)
    payload_hash = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())
    __table_args__ = (
        UniqueConstraint("flow_id", "version", name="uq_flow_version"),
        # (선택) 둘 다 true 금지
        CheckConstraint("NOT (is_current AND is_draft)", name="ck_current_xor_draft")
    )

    flow = relationship("Flow", back_populates="flow_snapshots")

# class FlowVersion(BaseDB):
#     __tablename__ = "flow_version"
#
#     id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
#     flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
#     version = Column(Integer, default=1)
#     is_draft = Column(Boolean, default=False)
#     created_at = Column(DateTime, default=func.now())
#     updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
#
#     flow_trigger_queues = relationship("FlowTriggerQueue", back_populates="flow_version", cascade="all, delete-orphan",
#                                        passive_deletes=True)
#     airflow_dag_run_histories = relationship("AirflowDagRunHistory", back_populates="flow_version",
#                                              cascade="all, delete-orphan",
#                                              passive_deletes=True)
#
#     def set_tasks(self, tasks):
#         self.tasks = tasks
#
#     def set_edges(self, edges):
#         self.edges = edges
#
#     def __repr__(self):
#         return (f"<FlowVersion ("
#                 f"  id={self.id}"
#                 f"  flow_id={self.flow_id}"
#                 f"  version={self.version}"
#                 f"  is_draft={self.is_draft}"
#                 f"  hash={self.hash}"
#                 f")>")
