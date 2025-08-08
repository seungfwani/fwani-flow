import uuid

from sqlalchemy import Column, String, DateTime, func, ForeignKey, Integer, JSON
from sqlalchemy.orm import relationship

from core.database import BaseDB


class FlowExecutionQueue(BaseDB):
    __tablename__ = "flow_execution_queue"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    flow_id = Column(String, ForeignKey("flow.id", ondelete="CASCADE"), nullable=False)
    dag_id = Column(String)
    run_id = Column(String)
    status = Column(String, default="waiting")
    scheduled_time = Column(DateTime, default=func.now())  # 실행 예정 시간, 즉시 실행시 의미 없음
    triggered_time = Column(DateTime, default=func.now())  # airflow trigger api 호출 시간
    data = Column(JSON, nullable=True)
    try_count = Column(Integer, default=0)
    file_hash = Column(String)  # 트리거 시점의 파일 해쉬, 트리거 시점에 파일 해쉬가 다르면 실행 중지
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    flow = relationship("Flow", back_populates="flow_execution_queues")
