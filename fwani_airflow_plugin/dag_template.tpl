"""
template 을 통해 만들어진 DAG 입니다.
"""
import sys
from datetime import datetime
from functools import wraps

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

sys.path.append({{ udf_folder_abspath }})
{{ import_udfs }}


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

{{ task_definitions }}
{{ task_rule }}
