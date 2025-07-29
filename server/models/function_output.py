import uuid

from sqlalchemy import Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship

from core.database import BaseDB


class FunctionOutput(BaseDB):
    __tablename__ = "function_output"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    function_id = Column(String, ForeignKey("function_library.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    description = Column(Text, nullable=True)

    function_ = relationship("FunctionLibrary", back_populates="output")
