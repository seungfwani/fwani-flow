import uuid

from sqlalchemy import Column, String, Text, DateTime, func, Integer, ForeignKey
from sqlalchemy.orm import relationship

from core.database import BaseDB


class Flow(BaseDB):
    __tablename__ = "flow"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True)
    description = Column(Text)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    owner_id = Column(String, ForeignKey("user.id"))

    owner = relationship("User", back_populates="flows")
    versions = relationship("FlowVersion", back_populates="flow", cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return (f"<Flow ("
                f"  id={self.id}"
                f"  name={self.name}"
                f"  description={self.description}"
                f")>")
