import uuid

from sqlalchemy import Column, String, DateTime, func, Integer, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from core.database import Base


class FlowVersion(Base):
    __tablename__ = "flow_version"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, default=1)
    is_draft = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    hash = Column(String)
    file_hash = Column(String)
    is_loaded_by_airflow = Column(Boolean, default=False)
    schedule = Column(String, default=None)

    flow = relationship("Flow", back_populates="versions")
    tasks = relationship("Task", back_populates="flow_version", cascade="all, delete-orphan", passive_deletes=True)
    edges = relationship("Edge", back_populates="flow_version", cascade="all, delete-orphan", passive_deletes=True)
    flow_trigger_queues = relationship("FlowTriggerQueue", back_populates="flow_version", cascade="all, delete-orphan",
                                       passive_deletes=True)
    airflow_dag_run_histories = relationship("AirflowDagRunHistory", back_populates="flow_version",
                                             cascade="all, delete-orphan",
                                             passive_deletes=True)

    def set_tasks(self, tasks):
        self.tasks = tasks

    def set_edges(self, edges):
        self.edges = edges

    def __repr__(self):
        return (f"<FlowVersion ("
                f"  id={self.id}"
                f"  flow_id={self.flow_id}"
                f"  version={self.version}"
                f"  is_draft={self.is_draft}"
                f"  hash={self.hash}"
                f")>")
