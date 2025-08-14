import uuid

from sqlalchemy import Column, String, JSON, Text, Boolean, func, DateTime

from core.database import BaseDB


class SystemFunction(BaseDB):
    __tablename__ = 'system_function'

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, index=True)
    description = Column(Text, nullable=True)
    impl_namespace = Column(String, nullable=False)
    impl_callable = Column(String, nullable=False)
    python_libraries = Column(JSON)

    # 인자 정의
    param_schema = Column(JSON, nullable=False, default={})
    is_deprecated = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
