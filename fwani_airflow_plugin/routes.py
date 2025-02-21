import base64
import os
import uuid

from airflow.configuration import conf
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify
from jinja2 import Template

fwani_api_bp = Blueprint('fwani_api', __name__, url_prefix='/api/v1/fwani')
# CSRF 비활성화: Blueprint 전체
csrf.exempt(fwani_api_bp)

ALLOWED_EXTENSIONS = ['py']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fwani_api_bp.route('/hello', methods=['GET'])
def hello():
    """
    Hello World
    ---
    responses:
      200:
        description: Hello World
    """
    return 'Hello World!'


@fwani_api_bp.route('/udf', methods=['POST'])
async def upload_udf():
    """
    Upload a python UDF file
    ---
    consumes:
      - multipart/form-data
    parameters:
      - in: formData
        name: file
        type: file
        required: true
        description: The file to upload
    responses:
      201:
        description: File uploaded successfully
      400:
        description: Invalid request
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']

    if not allowed_file(file.filename):
        return jsonify(
            {"error": f"Only {', '.join(map(lambda x: f'.{x}', ALLOWED_EXTENSIONS))} files are allowed"}), 400

    file_path = os.path.join(conf.get("core", "udf_folder"), file.filename)
    file.save(file_path)
    return jsonify({"message": f"{file.filename} UDF file uploaded successfully"}), 200


@fwani_api_bp.route('/udf', methods=['GET'])
def list_udf():
    """
    UDF list
    ---
    responses:
      200:
        description: UDF list
    """
    folder_path = conf.get("core", "udf_folder")
    files = os.listdir(folder_path)
    return jsonify({"files": files})


@fwani_api_bp.route('/dag', methods=['POST'])
def create_dag():
    """
    create dag
    ---
    consumes:
    - application/json
    parameters:
    - in: body
      name: body
      required: true
      description: JSON data for DAG (Node-Edge Graph 형태)
      schema:
        type: object
        properties:
          nodes:
            type: array
            description: UDF 노드 목록
            items:
              type: object
              properties:
                id:
                  type: string
                  description: 노드의 고유 ID (task_id 역할)
                filename:
                  type: string
                  description: 실행할 UDF 파일 이름
                function:
                  type: string
                  description: 실행할 함수 이름
          edges:
            type: array
            description: 노드 간 연결 관계 (의존성)
            items:
              type: object
              properties:
                from:
                  type: string
                  description: 부모 노드 ID
                to:
                  type: string
                  description: 자식 노드 ID
        example:
          nodes:
          - id: "task_A"
            filename: "udf_1"
            function: "fetch_data"
          - id: "task_B"
            filename: "udf_2"
            function: "process_data"
          - id: "task_C"
            filename: "udf_3"
            function: "store_data"
          edges:
          - from: "task_A"
            to: "task_B"
          - from: "task_A"
            to: "task_C"
          - from: "task_B"
            to: "task_C"
    responses:
      201:
        description: created dag successfully
      400:
        description: Invalid request
    """
    plugin_directory = os.path.dirname(os.path.abspath(__file__))
    UDF_FOLDER = conf.get("core", "udf_folder")
    DAG_FOLDER = conf.get("core", "dags_folder")
    data = request.json
    dag_id = "dag_" + base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
    nodes = data.get('nodes', [])  # list of node definition
    edges = data.get('edges', [])  # list of edges
    initial_kwargs = data.get("initial_kwargs", {})

    missing_udfs = [node for node in nodes if
                    not os.path.exists(os.path.join(UDF_FOLDER, node.get('filename') + ".py"))]
    if missing_udfs:
        return jsonify({"error": f"UDFs not found: {missing_udfs}"}), 400

    # Generate DAG content
    tasks = []
    # create tasks
    for i, node in enumerate(nodes):
        current_task_id = node.get("id")
        udf_filename = node.get("filename")
        udf_function = node.get("function")

        # ✅ 첫 번째 노드인지 확인
        is_first_task = all(edge["to"] != current_task_id for edge in edges)

        if is_first_task:
            python_callable = f"xcom_decorator({udf_function})"
            options = f"op_kwargs={initial_kwargs}"
        else:
            # ✅ 부모 노드를 찾아서 before_task_id 설정
            parent_tasks = [edge["from"] for edge in edges if edge["to"] == current_task_id]
            python_callable = f"""lambda **kwargs: xcom_decorator({udf_function})(
                before_task_ids={parent_tasks})"""
            options = ""

        tasks.append({
            "function": udf_function,
            "filename": udf_filename,
            "task_variable_name": current_task_id,
            "current_task_id": current_task_id,
            "python_callable": python_callable,
            "options": options,
        })
    task_rules = []
    for edge in edges:
        from_task = edge['from']
        to_task = edge['to']
        task_rules.append(f"{from_task} >> {to_task}")

    # create dag
    with open(os.path.join(plugin_directory, "dag_template.tpl"), "r") as f:
        template_str = f.read()
    dag_template = Template(template_str)

    filled_code = dag_template.render(
        dag_id=dag_id,
        task_rules=task_rules,
        tasks=tasks,
    )

    # write dag
    dag_file_path = os.path.join(DAG_FOLDER, f"{dag_id}.py")
    with open(dag_file_path, 'w') as dag_file:
        dag_file.write(filled_code)
        # dag_file.write('\n'.join(dag_content))

    return jsonify({
        "message": f"DAG {dag_id} created successfully",
        "dag_file": dag_file_path
    }), 201
