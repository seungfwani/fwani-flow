import ast


def validate_udf(file_path: str) -> bool:
    """
    UDF 파일의 유효성을 검사하는 함수

    :param file_path: 검사할 UDF 파일 경로
    :return: 유효한 경우 True, 그렇지 않으면 False
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()

        # 1. 파싱 오류 확인 (구문 오류 체크)
        parsed_tree = ast.parse(file_content, filename=file_path)

        # 2. 'run' 함수 존재 여부 확인
        run_function_node: ast.FunctionDef = next(
            (node for node in parsed_tree.body if isinstance(node, ast.FunctionDef) and node.name == 'run'),
            None
        )
        if run_function_node is None:
            print(f"❌ UDF 파일에 'run' 함수가 없음: {file_path}")
            return False

        # 3. 'return' 키워드 포함 여부 확인
        has_return_statement = any(
            isinstance(n, ast.Return) for n in ast.walk(run_function_node)
        )
        if not has_return_statement:
            print(f"❌ UDF 파일의 'run' 함수에서 반환값이 없음: {file_path}")
            return False

        print(f"✅ UDF 검증 통과: {file_path}")
        return True

    except SyntaxError:
        print(f"❌ UDF 파일의 문법 오류: {file_path}")
        return False

    except Exception as e:
        print(f"❌ UDF 검증 중 예외 발생: {e}")
        return False
