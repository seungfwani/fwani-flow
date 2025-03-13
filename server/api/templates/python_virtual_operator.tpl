{% set requirements_file = task.function.path ~ '/' ~ task.function.dependencies -%}

{{ task.variable_id }} = PythonVirtualenvOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable={{ task.decorator }}(
        inputs={{ task.decorator_parameters }}
    )(execute_udf),
    {% else %}
    python_callable={{ task.function.name }}_{{ task.function.function }}},
    {% endif -%}
    op_args=["{{ task.function.name }}", "{{ task.function.function }}"],
    op_kwargs={
        {% set options = task.options | from_json -%}
        {% for k, v in options.items() -%}
        "{{ k }}": {% if v is string %}"{{ v }}"{% else %}{{ v }}{% endif %},
        {% endfor -%}
        "operator_type": "airflow",
    },
    system_site_packages=False,
    requirements=[
        {% for package in get_udf_requirements(requirements_file) -%}
        "{{ package }}",
        {% endfor -%}
    ],
)
