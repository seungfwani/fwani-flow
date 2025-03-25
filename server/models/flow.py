import uuid

from sqlalchemy import Column, String, Text, DateTime, func, Integer
from sqlalchemy.orm import relationship

from core.database import Base


class Flow(Base):
    __tablename__ = "flow"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True)
    description = Column(Text)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    tasks = relationship("Task", back_populates="flow", cascade="all, delete-orphan", passive_deletes=True)
    edges = relationship("Edge", back_populates="flow", cascade="all, delete-orphan", passive_deletes=True)

    def add_task(self, task_data):
        self.tasks.append(task_data)

    def add_edge(self, edge_data):
        self.edges.append(edge_data)

    def __repr__(self):
        return (f"<Flow ("
                f"  id={self.id}"
                f"  name={self.name}"
                f"  description={self.description}"
                f"  version={self.version}"
                f")>")