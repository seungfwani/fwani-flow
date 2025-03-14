{% set requirements_file = task.function.path ~ '/' ~ task.function.dependencies -%}

{{ task.variable_id }} = PythonVirtualenvOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable=wrapped_callable,
    {% else %}
    python_callable={{ task.function.name }}_{{ task.function.function }}},
    {% endif -%}
    op_args=["{{ task.function.name }}", "{{ task.function.function }}"],
    op_kwargs={
        {% set options = task.options | from_json -%}
        {% for k, v in options.items() -%}
        "{{ k }}": {% if v is string %}"{{ v }}"{% else %}{{ v }}{% endif %},
        {% endfor -%}
        "operator_type": "{{ task.function.operator_type }}",
        {% raw -%}
        "dag_id": "{{ ti.dag_id }}",
        "task_id": "{{ ti.task.task_id }}",
        "is_last_task": "{% if ti.task.downstream_list | length == 0 %}True{% else %}False{% endif %}",
        "is_first_task": "{% if ti.task.upstream_list | length == 0 %}True{% else %}False{% endif %}",
        {% endraw -%}
    },
    system_site_packages=False,
    requirements=[
        {% for package in get_udf_requirements(requirements_file) -%}
        "{{ package }}",
        {% endfor -%}
    ],
)
