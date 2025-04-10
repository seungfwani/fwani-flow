import uuid

from sqlalchemy import Column, String, DateTime, func, ForeignKey, Integer
from sqlalchemy.orm import relationship

from core.database import Base


class FlowTriggerQueue(Base):
    __tablename__ = "flow_trigger_queue"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_version_id = Column(String, ForeignKey("flow_version.id", ondelete="CASCADE"), nullable=False)
    dag_id = Column(String)
    run_id = Column(String)
    status = Column(String, default="waiting")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now())
    data = Column(String, nullable=True)
    try_count = Column(Integer, default=0)

    flow_version = relationship("FlowVersion", back_populates="flow_trigger_queues")
