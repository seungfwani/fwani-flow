from functools import wraps

from airflow.operators.python import get_current_context


def xcom_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        context = get_current_context()
        ti = context['ti']

        task = ti.task
        upstream_tasks = task.upstream_list
        downstream_tasks = task.downstream_list

        is_first_task = len(list(upstream_tasks)) == 0
        if is_first_task:
            before_task_outputs = []
            for t_id, o_key in kwargs.get("before_task_ids", []):
                before_task_outputs.append(ti.xcom_pull(task_ids=t_id, key="output"))
            input_data = before_task_outputs
        else:
            input_data = kwargs.get("input_data")

        result = func(input_data, *args, **kwargs)

        is_last_task = len(list(downstream_tasks)) == 0
        if is_last_task:
            print(f"마지막 태스크임. output = {result}")
        else:
            ti.xcom_push(key="output", value=result)
        return result

    return wrapper


def rabbitmq_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
