import uuid

from sqlalchemy import Column, String, DateTime, func, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship

from core.database import BaseDB


class Task(BaseDB):
    __tablename__ = "task"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_version_id = Column(String, ForeignKey("flow_version.id", ondelete="CASCADE"), nullable=False)
    variable_id = Column(String, nullable=False)

    python_libraries = Column(JSON)
    code_string = Column(Text)
    code_hash = Column(String)

    # ui node data
    ui_type = Column(String, nullable=True, default="default")
    ui_label = Column(String, nullable=True)
    ui_position = Column(JSON, nullable=True, default={"x": 0, "y": 0})
    ui_style = Column(JSON, nullable=True, default={})
    ui_extra_data = Column(JSON, nullable=True, default={})

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    flow_version = relationship("FlowVersion", back_populates="tasks")
    inputs = relationship("TaskInput", back_populates="task", cascade="all, delete-orphan")


class TaskInput(BaseDB):
    __tablename__ = "task_input"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    key = Column(String, nullable=False)
    type = Column(String, nullable=False, default="string", server_default="string")
    value = Column(String, nullable=True)

    task = relationship("Task", back_populates="inputs")
