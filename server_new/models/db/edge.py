import uuid

from sqlalchemy import Column, String, ForeignKey, JSON, Numeric
from sqlalchemy.orm import relationship

from core.database import BaseDB


class Edge(BaseDB):
    __tablename__ = "edge"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
    from_task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"))
    to_task_id = Column(String, ForeignKey("task.id", ondelete="CASCADE"))

    flow = relationship("Flow", back_populates="edges")
    from_task = relationship("Task", foreign_keys=from_task_id)
    to_task = relationship("Task", foreign_keys=to_task_id)

    # edge ui data
    ui_type = Column(String, nullable=True, default="default")
    ui_label = Column(String, nullable=True, default=None)
    ui_labelStyle = Column(JSON, nullable=True, default={})
    ui_labelBgStyle = Column(JSON, nullable=True, default={})
    ui_labelBgPadding = Column(JSON, nullable=True, default=[])
    ui_labelBgBorderRadius = Column(Numeric, nullable=True, default=0)
    ui_style = Column(JSON, nullable=True, default={})

