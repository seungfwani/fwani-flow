import json
import uuid

from sqlalchemy import Column, String, DateTime, Boolean, func, ForeignKey
from sqlalchemy.orm import relationship

from core.database import Base
from models.flow_version import FlowVersion
from utils.functions import string2datetime


class AirflowDagRunHistory(Base):
    __tablename__ = "airflow_dag_run_history"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    flow_version_id = Column(String, ForeignKey("flow_version.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(String, nullable=False)
    execution_date = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(String)  # e.g., 'success', 'failed'
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String)  # e.g., 'manual', 'scheduled'
    conf = Column(String)
    source = Column(String, nullable=True)  # e.g. 'ui', 'api', 'cli'
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now())

    flow_version = relationship("FlowVersion")

    @classmethod
    def from_json(cls, flow_version: FlowVersion, data: dict) -> "AirflowDagRunHistory":
        return AirflowDagRunHistory(
            id=str(uuid.uuid4()),
            run_id=data["dag_run_id"],
            execution_date=string2datetime(data.get("execution_date")),
            start_date=string2datetime(data.get("start_date")),
            end_date=string2datetime(data.get("end_date")),
            status=data.get("state"),
            external_trigger=data.get("external_trigger"),
            run_type=data.get("run_type"),
            conf=json.dumps(data.get("conf")),
            source=data.get("conf", {}).get("source", "airflow"),
            flow_version=flow_version,
        )
