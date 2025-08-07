import uuid

from sqlalchemy import Column, String, JSON, Text

from core.database import BaseDB


class FunctionTemplate(BaseDB):
    __tablename__ = "function_template"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    python_libraries = Column(JSON)
    code_string = Column(Text, nullable=True)
    code_hash = Column(String, nullable=True)
