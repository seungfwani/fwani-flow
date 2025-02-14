import base64
import os
import uuid
from textwrap import dedent

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
      description: JSON data for DAG
      schema:
        type: object
        properties:
          udf_sequence:
            type: array
            items:
              type: object
              properties:
                filename:
                  type: string
                function:
                  type: string
        example:
          udf_sequence:
          - filename: "string"
            function: "string"
    responses:
      201:
        description: created dag successfully
      400:
        description: Invalid request
    """
    UDF_FOLDER = conf.get("core", "udf_folder")
    DAG_FOLDER = conf.get("core", "dags_folder")
    data = request.json
    dag_id = "dag_" + base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
    udf_sequence = data.get("udf_sequence")  # List of UDFs in order
    initial_kwargs = data.get("initial_kwargs", {})

    if not udf_sequence:
        return jsonify({"error": "udf_sequence are required"}), 400

    missing_udfs = [udf for udf in udf_sequence if
                    not os.path.exists(os.path.join(UDF_FOLDER, udf.get('filename') + ".py"))]
    if missing_udfs:
        return jsonify({"error": f"UDFs not found: {missing_udfs}"}), 400

    # Generate DAG content
    tasks = []
    current_task_id_param = "first"
    for i, udf in enumerate(udf_sequence):
        before_task_variable_name = f"task_{i}"
        before_task_id = f"{before_task_variable_name}_{current_task_id_param}"

        task_variable_name = f"task_{i + 1}"
        udf_filename = udf.get("filename")
        udf_function = udf.get("function")
        current_task_id_param = f"{udf_filename}_{udf_function}"
        current_task_id = f"{task_variable_name}_{current_task_id_param}"
        if i == 0:
            python_operator_options = f"""
                python_callable=airflow_udf_decorator({udf_function}),
                op_kwargs={initial_kwargs},
            """
        else:
            python_operator_options = f"""
                python_callable=lambda **kwargs: airflow_udf_decorator({udf_function})(
                before_task_id="{before_task_id}"),
            """
        tasks.append({
            "name": task_variable_name,
            "function": udf_function,
            "filename": udf_filename,
            "code": dedent(f"""\
            {task_variable_name} = PythonOperator(
                task_id='{current_task_id}',
                dag=dag,
                """ + python_operator_options + ")"),
        })

    # Create dependencies
    # dependencies = ""
    # for i in range(len(tasks) - 1):
    #     dependencies += f"task_{i + 1}.set_downstream(task_{i + 2})\n"

    import_udf = "\n".join(
        [f"from {task.get("filename")} import {task.get("function")}" for i, task in enumerate(tasks)])
    task_definitions = "\n".join([task.get("code") for task in tasks])
    task_sequence = " >> ".join([task.get("name") for task in tasks])
    # dag_content = (
    #     dedent("""
    #     import sys
    #     from datetime import datetime
    #     from functools import wraps
    #
    #     from airflow import DAG
    #     from airflow.operators.python import PythonOperator, get_current_context
    # """),
    #     f'sys.path.append("{os.path.abspath(UDF_FOLDER)}")',
    #     f'{import_udf}',
    #     dedent("""
    #
    # def airflow_udf_decorator(f):
    #     @wraps(f)
    #     def wrapper(*args, **kwargs):
    #         print("🔍 Received args:", args)  # 디버깅용 출력
    #         print("🔍 Received kwargs:", kwargs)  # 디버깅용 출력
    #         # TaskInstance 가져오기
    #         context = get_current_context()
    #         ti = context["ti"]
    #         before_task_id = kwargs.get("before_task_id", None)
    #         if before_task_id is None:
    #             print("⚠️ Warning: `before_task_id` is missing in kwargs!")
    #             result = f(*args, **kwargs)
    #         else:
    #             # 이전 태스크 출력값 가져오기 (XCom)
    #             in_data = ti.xcom_pull(task_ids=before_task_id, key='return_value')
    #             result = f(in_data)
    #         # 결과값 XCom에 저장
    #         ti.xcom_push(key='return_value', value=result)
    #         return result
    #     return wrapper
    #
    #
    # default_args = {
    #     'owner': 'airflow',
    #     'start_date': datetime(2023, 1, 1),
    #     'retries': 1,
    # }
    # """),
    #     dedent(f"""
    # dag = DAG(
    #     dag_id='{dag_id}',
    #     default_args=default_args,
    #     schedule_interval=None,
    #     catchup=False,
    # )
    # """),
    #     f'{task_definitions}',
    #     f'{task_sequence}',
    # )
    # {dependencies}

    with open("dag_template.tpl", "r") as f:
        template_str = f.read()
    dag_template = Template(template_str)

    filled_code = dag_template.render(
        udf_folder_abspath=os.path.abspath(UDF_FOLDER),
        import_udfs=import_udf,
        dag_id=dag_id,
        task_definitions=task_definitions,
        task_rule=task_sequence,
    )

    dag_file_path = os.path.join(DAG_FOLDER, f"{dag_id}.py")
    with open(dag_file_path, 'w') as dag_file:
        dag_file.write(filled_code)
        # dag_file.write('\n'.join(dag_content))

    return jsonify({
        "message": f"DAG {dag_id} created successfully",
        "dag_file": dag_file_path
    }), 201
