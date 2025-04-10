from typing import Optional

from pydantic import BaseModel, Field

from models.flow_trigger_queue import FlowTriggerQueue


class TriggerResponse(BaseModel):
    id: str = Field(..., description="trigger id", examples=["00000000-0000-4000-9000-000000000000"])
    flow_version_id: str
    airflow_dag_id: str
    status: str
    data: Optional[str]

    @classmethod
    def from_flow_trigger_queue(cls, ftq: FlowTriggerQueue):
        return cls(
            id=ftq.id,
            flow_version_id=ftq.flow_version_id,
            airflow_dag_id=ftq.dag_id,
            status=ftq.status,
            data=ftq.data,
        )
