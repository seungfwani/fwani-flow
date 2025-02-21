"""
template 을 통해 만들어진 DAG 입니다.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from fwani_airflow_plugin.decorator import xcom_decorator

{% for task in tasks -%}
from {{ task.filename }} import {{ task.function }}
{% endfor %}

default_args = {
    'owner': 'code_generator',
    'start_date': datetime.now(),
    'retries': 1,
}

dag = DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
{% for task in tasks %}
{{ task.task_variable_name }} = PythonOperator(
    task_id='{{ task.current_task_id }}',
    dag=dag,
    python_callable={{ task.python_callable }},
    {{ task.options }}
)
{% endfor %}

{% for rule in task_rules -%}
{{ rule }}
{% endfor %}
