# Workflow

본 프로젝트는 UDF(사용자 정의 함수)를 이용하여 DAG 를 생성하고 실행, 관리 할 수 있는 Workflow 를 개발하는 것이 목표입니다.
사용자는 UI 를 통해 쉽게 Workflow 를 정의하고 결과를 확인 할 수 있습니다.

## Overview

- [Overview](./docs/00.%20overview.md)

## 주요 기능

- [기능 요구사항 명세서](./docs/01.%20Requirements/01.%20Functional_Requirements.md)
- [비기능 요구사항 명세서](./docs/01.%20Requirements/02.%20NonFunctional_Requirements.md)

## 프로젝트 구조

- [프로젝트 시스템 아키텍처](./docs/02.%20Design/01.%20architecture.md)

### 프로젝트 파일 구조

```text
.
├── Dockerfile                              # airflow + workflow plugin dockerfile
├── README.md
├── airflow.cfg                             # airflow 설정 파일
├── docker-compose-airflow-standalone.yaml  # docker-compose 배포 파일
├── entrypoint.sh                           # airflow + workflow plugin entrypoint
├── docs
│     ├── 00. 요구사항.md
│     ├── 01. architecture.md
│     ├── 02. database.md
│     ├── 03. api.md
│     ├── Workflow.drawio
│     └── imgs
│         ├── architecture.png
│         └── table_schema.png
├── example                                 # 예제 udf 파일
│     ├── example_udf_fetch.py
│     ├── example_udf_finish.py
│     ├── example_udf_process1.py
│     └── example_udf_process2.py
├── front                                   # Frontend 파일
│     ├── README.md
│     ├── babel.config.js
│     ├── jsconfig.json
│     ├── package-lock.json
│     ├── package.json
│     ├── public
│     │     ├── favicon.ico
│     │     └── index.html
│     ├── src
│     │     ├── App.vue
│     │     ├── api
│     │     │     ├── dag.js
│     │     │     └── udf.js
│     │     ├── assets
│     │     │     └── logo.png
│     │     ├── components
│     │     │     ├── CustomNode.vue
│     │     │     ├── DAGEditor.vue
│     │     │     ├── DAGList.vue
│     │     │     ├── DropzoneBackground.vue
│     │     │     ├── FlowSideBar.vue
│     │     │     ├── HelloWorld.vue
│     │     │     ├── UDFList.vue
│     │     │     └── UDFUpload.vue
│     │     ├── main.js
│     │     └── scripts
│     │         ├── editer.css
│     │         └── useDnD.js
│     └── vue.config.js
├── fwani_airflow_plugin                        # 플러그인 버전 (사용 안할 가능성 높음)
│     ├── __init__.py
│     ├── dag_template.tpl
│     ├── debug_router.py
│     ├── decorator.py
│     ├── requirements.txt
│     ├── routes.py
│     └── swagger.py
├── requirements.txt
├── server                                      # 서버 코드
│     ├── __init__.py
│     ├── alembic
│     │     ├── README
│     │     ├── env.py
│     │     ├── script.py.mako
│     │     └── versions
│     │         └── 4ede38919f46_initial_migration.py
│     ├── alembic.ini
│     ├── api
│     │     ├── models
│     │     │     └── dag_model.py
│     │     └── routers
│     │         ├── __init__.py
│     │         ├── dag_router.py
│     │         └── udf_router.py
│     ├── config.py
│     ├── core
│     │     ├── database.py
│     │     └── log.py
│     ├── dag_template.tpl
│     ├── data
│     │     ├── dag
│     │     │     ├── dag_ZGF0MTI.py
│     │     │     ├── dag_ZGF0MTIz.py
│     │     │     └── dag_dGVzdF9kMjJhZw.py
│     │     ├── logs
│     │     │     └── app.log
│     │     └── udf
│     │         ├── example_udf_fetch_25bcc7.py
│     │         ├── example_udf_fetch_721477.py
│     │         └── example_udf_fetch_ab90e6.py
│     ├── env.sh
│     ├── main.py
│     ├── models
│     │     ├── edge.py
│     │     ├── flow.py
│     │     ├── function_library.py
│     │     └── task.py
│     ├── requirements.txt
│     ├── utils
│     │     ├── functions.py
│     │     └── udf_validator.py
│     └── workflow.db

26 directories, 79 files

```

---

## 설치 및 실행

- [설치 배포 문서](./docs/05.%20Deployment/01.%20deployment.md)

## DAG 예제

- API를 통해 생성하는 DAG 예제

```json
{
  "name": "test_dag",
  "description": "test dag 입니다.",
  "nodes": [
    {
      "id": "task1",
      "function_id": "47be04a6-6ae2-402e-ad8a-5e644ed57d43"
    },
    {
      "id": "task2",
      "function_id": "47be04a6-6ae2-402e-ad8a-5e644ed57d43"
    }
  ],
  "edges": [
    {
      "from": "task1",
      "to": "task2"
    }
  ]
}
```

- 생성된 DAG 코드

```rb
# ./server/dag_template.tpl
```
