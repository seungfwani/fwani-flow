import uuid

from sqlalchemy import Column, String, DateTime, func, Boolean
from sqlalchemy.orm import relationship

from core.database import BaseDB


class User(BaseDB):
    __tablename__ = "user"
    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    username = Column(String)
    email = Column(String, unique=True)
    hashed_password = Column(String)
    is_admin = Column(Boolean, default=False, server_default="false")
    created_at = Column(DateTime, default=func.now())

    flows = relationship("Flow", back_populates="owner")
