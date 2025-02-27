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
    python_callable = Column(Text, nullable=True)
    options = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    flow = relationship("Flow", back_populates="tasks")
    function = relationship("FunctionLibrary")
