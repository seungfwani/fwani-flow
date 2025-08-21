import logging

logger = logging.getLogger()

class WorkflowError(Exception):
    def __init__(self, message: str, code: str = "ERROR") -> None:
        self.message = message
        self.code = code
        super().__init__(self.message)
        logger.error(self.message)


class WorkflowUserError(WorkflowError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "USER ERROR")
