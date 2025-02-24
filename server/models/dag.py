import uuid

from sqlalchemy import Column, String, Text, DateTime, func, Integer
from sqlalchemy.orm import relationship

from core.database import Base


class DAG(Base):
    __tablename__ = "dags"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True)
    description = Column(Text)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    udf_relations = relationship("DAGUDFRelation", back_populates="dag")
