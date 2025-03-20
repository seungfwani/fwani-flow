import uuid

from sqlalchemy import Column, String, Text, DateTime, func
from sqlalchemy.orm import relationship

from core.database import Base


class FunctionLibrary(Base):
    __tablename__ = "function_library"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=True)
    main_filename = Column(String, nullable=False)
    path = Column(String, unique=True)
    function = Column(String)
    operator_type = Column(String, default="python")
    docker_image_tag = Column(String, nullable=True)
    dependencies = Column(String, nullable=True)
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    ui_type = Column(String, default="custom")

    inputs = relationship("FunctionInput", back_populates="function_", cascade="all, delete-orphan")
    output = relationship("FunctionOutput", back_populates="function_",
                          cascade="all, delete-orphan", uselist=False)
