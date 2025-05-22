from enum import Enum


class DataType(str, Enum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DICT = "dict"
    LIST = "list"
    VARIABLE_ARGS = "variable_args"
