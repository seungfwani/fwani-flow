import uuid

from sqlalchemy import Column, String, ForeignKey, JSON
from sqlalchemy.orm import relationship

from core.database import Base


class NodeUI(Base):
    __tablename__ = "node_ui"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    type = Column(String, index=True, nullable=False, default="default")
    position = Column(JSON, index=True, nullable=False, default={"x": 0, "y": 0})
    style = Column(JSON, index=True, nullable=False, default={})

    task = relationship("Task", uselist=False)
