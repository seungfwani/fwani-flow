import uuid

from sqlalchemy import Column, String, DateTime, func, ForeignKey, Text
from sqlalchemy.orm import relationship

from core.database import Base


class Task(Base):
    __tablename__ = "task"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
    function_id = Column(String, ForeignKey("function_library.id", ondelete="SET NULL"), nullable=True)
    variable_id = Column(String, nullable=False)
    decorator = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    flow = relationship("Flow", back_populates="tasks")
    inputs = relationship("TaskInput", back_populates="task")
    function = relationship("FunctionLibrary", uselist=False)
    task_ui = relationship("TaskUI", back_populates="task", uselist=False, cascade="all, delete-orphan")
