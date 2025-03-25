import uuid

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship

from core.database import Base


class TaskInput(Base):
    __tablename__ = "task_input"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    key = Column(String, nullable=False)
    type = Column(String, nullable=False, default="string", server_default="string")
    value = Column(String, nullable=True)

    task = relationship("Task", back_populates="inputs")
