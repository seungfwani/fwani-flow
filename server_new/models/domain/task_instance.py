from datetime import datetime

from models.domain.enums import FlowExecutionStatus


class TaskInstance:
    def __init__(self,
                 task_id: str,
                 execution_date: datetime,
                 start_date: datetime,
                 end_date: datetime,
                 duration: float,
                 operator: str,
                 queued_when: datetime,
                 status: FlowExecutionStatus,
                 try_number: int
                 ):
        self.task_id = task_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.end_date = end_date
        self.duration = duration
        self.operator = operator
        self.queued_when = queued_when
        self.status = status
        self.try_number = try_number

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "execution_date": self.execution_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "duration": self.duration,
            "operator": self.operator,
            "queued_when": self.queued_when,
            "status": self.status,
            "try_number": self.try_number,
        }