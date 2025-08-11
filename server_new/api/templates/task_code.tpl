"""
template 을 통해 만들어진 Code 입니다.
task code 는 함수 run 을 무조건 포함한다고 가정.
run 은 return 을 dataframe 만 한다.
"""



def wrapper_run(dag_id: str,
                run_id: str,
                task_id: str,
                before_task_ids: list[str],
                base_dir: str = "/app/shared"
):
    import os
    import pickle

{{ task_code }}

    RESULT_FILE_NAME = "result.pkl"
    dag_run_dir = os.path.join(base_dir,
                               f"dag_id={dag_id}",
                               f"run_id={run_id}", )

    # get input_dfs of user function
    input_dfs = []
    for tid in before_task_ids:
        path = os.path.join(dag_run_dir, f"task_id={tid}/{RESULT_FILE_NAME}")
        with open(path, "rb") as fi:
            input_dfs.append(pickle.load(fi))

    # call user function
    # 실행 대상 결정
    kind = "{{ kind }}"
    if kind == "code":
        # 사용자 코드에 정의된 run 사용
        result = run(*input_dfs)
    else:
        # 내장 템플릿 로딩
        mod = importlib.import_module("{{ impl_namespace }}")
        func = getattr(mod, "{{ impl_callable }}")
        result = func(*input_dfs, params={{ params }})

    # write result
    task_dir = os.path.join(dag_run_dir, f"task_id={task_id}")
    os.makedirs(task_dir, exist_ok=True)
    output_path = os.path.join(task_dir, RESULT_FILE_NAME)
    with open(output_path, "wb") as fo:
        pickle.dump(result, fo)
    return output_path
