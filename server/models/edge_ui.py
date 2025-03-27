import uuid

from sqlalchemy import Column, String, ForeignKey, JSON, Numeric
from sqlalchemy.orm import relationship

from core.database import Base


class EdgeUI(Base):
    __tablename__ = "edge_ui"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    edge_id = Column(String, ForeignKey("edge.id", ondelete="CASCADE"), nullable=False)
    type = Column(String, nullable=True, default="default")
    label = Column(String, nullable=True, default=None)
    labelStyle = Column(JSON, nullable=True, default={})
    labelBgStyle = Column(JSON, nullable=True, default={})
    labelBgPadding = Column(JSON, nullable=True, default=[])
    labelBgBorderRadius = Column(Numeric, nullable=True, default=0)
    style = Column(JSON, nullable=True, default={})

    edge = relationship("Edge", back_populates="edge_ui", uselist=False)
