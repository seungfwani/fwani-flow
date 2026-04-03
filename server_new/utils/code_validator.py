import ast

FORBIDDEN_MODULES = {"os", "sys"}
FORBIDDEN_FUNCTIONS = {"exec"}
REQUIRED_FUNCTION = "run"


class CodeValidator(ast.NodeVisitor):
    def __init__(self):
        self.has_run_function = False
        self.run_has_return = False
        self.imported_modules = set()
        self.used_forbidden_names = set()

    def visit_Import(self, node):
        for alias in node.names:
            self.imported_modules.add(alias.name.split('.')[0])
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module:
            self.imported_modules.add(node.module.split('.')[0])
        self.generic_visit(node)

    def visit_Name(self, node):
        if node.id in FORBIDDEN_MODULES | FORBIDDEN_FUNCTIONS:
            self.used_forbidden_names.add(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id in FORBIDDEN_MODULES:
            self.used_forbidden_names.add(node.value.id)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        if node.name == REQUIRED_FUNCTION:
            self.has_run_function = True
            if any(isinstance(n, ast.Return) for n in ast.walk(node)):
                self.run_has_return = True
        self.generic_visit(node)


def validate_user_code(code_str: str) -> list[str]:
    errors = []
    try:
        tree = ast.parse(code_str)
    except SyntaxError as e:
        return [f"SyntaxError: {e}"]

    validator = CodeValidator()
    validator.visit(tree)

    if not validator.has_run_function:
        errors.append("`run()` 함수가 정의되어야 합니다.")
    if not validator.run_has_return:
        errors.append("`run()` 함수에 `return` 문이 필요합니다.")
    if validator.imported_modules & FORBIDDEN_MODULES:
        errors.append(f"금지된 모듈 import 발견: {validator.imported_modules & FORBIDDEN_MODULES}")
    if validator.used_forbidden_names:
        errors.append(f"금지된 모듈 사용 발견: {validator.used_forbidden_names}")

    return errors
