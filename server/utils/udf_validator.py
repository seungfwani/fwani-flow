import ast
import logging
from typing import List

from fastapi import HTTPException

from models.function_input import FunctionInput

logger = logging.getLogger()


def validate_udf(file_path: str, function_name: str) -> bool:
    """
    UDF 파일의 유효성을 검사하는 함수

    :param file_path: 검사할 UDF 파일 경로
    :param function_name:
    :return: 유효한 경우 True, 그렇지 않으면 False
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()

        # 1. 파싱 오류 확인 (구문 오류 체크)
        parsed_tree = ast.parse(file_content, filename=file_path)

        # 2. 'run' 함수 존재 여부 확인
        run_function_node: ast.FunctionDef = next(
            (node for node in parsed_tree.body if isinstance(node, ast.FunctionDef) and node.name == function_name),
            None
        )
        if run_function_node is None:
            logger.warning(f"❌ UDF 파일에 '{function_name}' 함수가 없음: {file_path}")
            return False

        # 3. 'return' 키워드 포함 여부 확인
        has_return_statement = any(
            isinstance(n, ast.Return) for n in ast.walk(run_function_node)
        )
        if not has_return_statement:
            logger.warning(f"❌ UDF 파일의 '{function_name}' 함수에서 반환값이 없음: {file_path}")
            return False

        logger.info(f"✅ UDF 검증 통과: {file_path}")
        return True

    except SyntaxError:
        logger.error(f"❌ UDF 파일의 문법 오류: {file_path}")
        return False

    except Exception as e:
        logger.error(f"❌ UDF 검증 중 예외 발생: {e}")
        return False


def get_validated_inputs(udf_input_list: List[FunctionInput], user_input: dict):
    validated_inputs = []
    for udf_input in udf_input_list:
        if udf_input.name in user_input:
            value = user_input[udf_input.name]
        else:
            value = udf_input.default_value
        if udf_input.type == "list":
            if not value:
                value = "[]"
        elif udf_input.type == "dict":
            if not value:
                value = "{}"
        validated_inputs.append({
            "key": udf_input.name,
            "value": value,
            "type": udf_input.type
        })
    logger.info("validated inputs: " + str(validated_inputs))
    return validated_inputs


def validate_input_type(expected_type, key, value):
    try:
        if expected_type == "string":
            return str(value)
        elif expected_type == "int":
            return int(value)
        elif expected_type == "float":
            return float(value)
        elif expected_type == "bool":
            if isinstance(value, bool):
                return value
            elif value.lower() in ["true", "false"]:
                return value.lower() == "true"
            else:
                raise ValueError(f"Invalid input type: {expected_type}")
        else:
            raise ValueError(f"Invalid input type: {expected_type}")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid value for {key}: expected {expected_type}, got {value}")
