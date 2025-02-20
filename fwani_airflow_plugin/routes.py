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
    plugin_directory = os.path.dirname(os.path.abspath(__file__))
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
    # create tasks
    for i, udf in enumerate(udf_sequence):
        before_task_variable_name = f"task_{i}"
        before_task_id = f"{before_task_variable_name}_{current_task_id_param}"

        task_variable_name = f"task_{i + 1}"
        udf_filename = udf.get("filename")
        udf_function = udf.get("function")
        current_task_id_param = f"{udf_filename}_{udf_function}"
        current_task_id = f"{task_variable_name}_{current_task_id_param}"

        if i == 0:
            python_callable = f"xcom_decorator({udf_function})"
            options = f"op_kwargs={initial_kwargs}"
        else:
            python_callable = f"""lambda **kwargs: xcom_decorator({udf_function})(
                before_task_id="{before_task_id}")"""
            options = ""

        with open(os.path.join(plugin_directory, "task_template.tpl"), "r") as f:
            template_str = f.read()
        task_template = Template(template_str)

        tasks.append({
            "name": task_variable_name,
            "function": udf_function,
            "filename": udf_filename,
            "code": task_template.render(
                task_variable_name=task_variable_name,
                current_task_id=current_task_id,
                python_callable=python_callable,
                options=options,
            ),
        })

    # add task rule
    import_udf = "\n".join(
        [f"from {task.get('filename')} import {task.get('function')}" for i, task in enumerate(tasks)])
    task_definitions = "\n".join([task.get("code") for task in tasks])
    task_sequence = " >> ".join([task.get("name") for task in tasks])

    # create dag
    with open(os.path.join(plugin_directory, "dag_template.tpl"), "r") as f:
        template_str = f.read()
    dag_template = Template(template_str)
    decorator_import = "from fwani_airflow_plugin.decorator import xcom_decorator"

    filled_code = dag_template.render(
        import_udfs=import_udf,
        decorator_import=decorator_import,
        dag_id=dag_id,
        task_definitions=task_definitions,
        task_rule=task_sequence,
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
