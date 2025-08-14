import uuid

from sqlalchemy import Column, String, DateTime, func, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship

from core.database import BaseDB


class Task(BaseDB):
    __tablename__ = "task"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
    variable_id = Column(String, nullable=False)

    kind = Column(String)

    # 코드 타입 전용
    code_string = Column(Text)  # CODE 일 때만 사용
    code_hash = Column(String)

    # 공통 실행 스펙
    python_libraries = Column(JSON)
    system_function_id = Column(String, ForeignKey("system_function.id"), nullable=True, server_default=None)
    impl_namespace = Column(String)  # 내장 함수
    impl_callable = Column(String, default="run")  # "run" 같은 entrypoint 함수 (기본 "run")

    input_meta_type = Column(JSON)
    output_meta_type = Column(JSON)

    # UI
    ui_type = Column(String, nullable=True, default="default")
    ui_label = Column(String, nullable=True)
    ui_position = Column(JSON, nullable=True, default={"x": 0, "y": 0})
    ui_style = Column(JSON, nullable=True, default={})
    ui_class = Column(String, nullable=True)
    ui_extra_data = Column(JSON, nullable=True, default={})

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    flow = relationship("Flow", back_populates="tasks")
    inputs = relationship("TaskInput", back_populates="task", cascade="all, delete-orphan")
    system_function = relationship("SystemFunction", uselist=False)


class TaskInput(BaseDB):
    __tablename__ = "task_input"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    key = Column(String, nullable=False)
    type = Column(String, nullable=False, default="string", server_default="string")
    value = Column(String, nullable=True)

    task = relationship("Task", back_populates="inputs")
