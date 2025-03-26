import uuid

from sqlalchemy import Column, String, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from core.database import Base


class FunctionInput(Base):
    __tablename__ = "function_input"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    function_id = Column(String, ForeignKey("function_library.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    required = Column(Boolean, default=True)
    default_value = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    sensitive = Column(Boolean, default=False, server_default="false")

    function_ = relationship("FunctionLibrary", back_populates="inputs")
