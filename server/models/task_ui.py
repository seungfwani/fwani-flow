import uuid

from sqlalchemy import Column, String, ForeignKey, JSON
from sqlalchemy.orm import relationship

from core.database import Base


class TaskUI(Base):
    __tablename__ = "task_ui"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    type = Column(String, nullable=True, default="default")
    label = Column(String, nullable=True)
    position = Column(JSON, nullable=True, default={"x": 0, "y": 0})
    style = Column(JSON, nullable=True, default={})

    task = relationship("Task", back_populates="task_ui", uselist=False)
