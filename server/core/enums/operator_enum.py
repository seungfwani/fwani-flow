from enum import Enum


class OperatorType(str, Enum):
    PYTHON = "python"
    PYTHON_VIRTUALENV = "python_virtual"
    DOCKER = "docker"
