import uuid

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship

from core.database import BaseDB


class Edge(BaseDB):
    __tablename__ = "edge"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_version_id = Column(String, ForeignKey("flow_version.id", ondelete="CASCADE"), nullable=False)
    from_task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"))
    to_task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"))

    flow_version = relationship("FlowVersion", back_populates="edges")
    from_task = relationship("Task", foreign_keys=from_task_id)
    to_task = relationship("Task", foreign_keys=to_task_id)

    edge_ui = relationship("EdgeUI", back_populates="edge", uselist=False, cascade="all, delete-orphan")
