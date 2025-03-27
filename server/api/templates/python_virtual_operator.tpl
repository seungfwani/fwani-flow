{% set requirements_file = task.function.path ~ '/' ~ task.function.dependencies -%}

{{ task.variable_id }} = PythonVirtualenvOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable=wrapped_callable,
    {% else %}
    python_callable={{ task.function.main_filename }}_{{ task.function.function }}},
    {% endif -%}
    op_args=["{{ task.function.name }}", "{{ task.function.main_filename }}", "{{ task.function.function }}"],
    op_kwargs={
        {% for inp in task.inputs -%}
        "{{ inp.key }}": {% if inp.type == "string" %}"{{ inp.value }}"{% else %}{{ inp.value }}{% endif %},
        {% endfor -%}
        "operator_type": "{{ task.function.operator_type }}",
        "input_schema": [
        {% for inp in task.function.inputs -%}
            {"name": "{{ inp.name }}", "type": "{{ inp.type }}"},
        {% endfor -%}
        ],
        {% raw -%}
        "dag_id": "{{ ti.dag_id }}",
        "run_id": "{{ ti.run_id }}",
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
