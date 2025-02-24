from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.orm import relationship

from core.database import Base


class DAGUDFRelation(Base):
    __tablename__ = "dag_udf_relations"

    dag_id = Column(String, ForeignKey("dags.id"), primary_key=True)
    udf_id = Column(String, ForeignKey("udfs.id"), primary_key=True)
    order = Column(Integer)

    dag = relationship("DAG", back_populates="udf_relations")
    udf = relationship("UDF")
