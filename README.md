# Workflow

이 프로젝트는 Airflow Plugin을 활용하여 UDF(사용자 정의 함수)와 DAG(워크플로우)를 관리하는 시스템입니다.
사용자는 자신만의 UDF를 업로드하고, 이를 조합하여 DAG을 생성하고 실행할 수 있습니다.
또한 REST API를 통해 DAG과 UDF를 손쉽게 관리할 수 있습니다.

## 주요 기능

### 1. UDF 업로드 및 조회

- 사용자가 `Python`으로 작성한 UDF 파일을 업로드할 수 있습니다.
- 업로드된 UDF는 특정 경로에 저장되며, DAG에서 활용 가능합니다.
- API를 통해 업로드된 UDF 목록을 조회할 수 있습니다.

### 2. DAG 생성 및 실행

- 사용자가 업로드한 UDF를 활용하여 DAG을 생성 및 실행할 수 있습니다.
- DAG의 노드(Node) 및 엣지(Edge)를 지정하여 다양한 워크플로우를 구성할 수 있습니다.
- 의존 관계(Dependency)를 반영하여 DAG을 실행할 수 있습니다.

### 3. DAG 그래프 기반 워크플로우

- 단순 순차 실행뿐만 아니라, 병렬 실행 및 DAG 구조를 지원합니다.
- `task_A >> [task_B, task_C]` 와 같이 동시에 실행되는 태스크를 구성할 수 있습니다.
- DAG의 의존성을 설정하여 동작 순서를 조정할 수 있습니다.

### 4. API 기반 관리

- REST API를 통해 DAG, UDF 업로드 및 조회가 가능합니다.
- API를 활용하여 자동화된 DAG 관리 및 배포 시스템을 구축할 수 있습니다.

---

## 프로젝트 구조

```text
.
├── Dockerfile                              # airflow + workflow plugin dockerfile
├── README.md
├── airflow.cfg                             # airflow 설정 파일
├── docker-compose-airflow-standalone.yaml  # docker-compose 배포 파일
├── entrypoint.sh                           # airflow + workflow plugin entrypoint
├── example                                 # 예제 udf 파일
│         ├── example_udf_fetch.py
│         ├── example_udf_finish.py
│         ├── example_udf_process1.py
│         └── example_udf_process2.py
├── fwani_airflow_plugin
│         ├── __init__.py                   # airflow plugin 설정
│         ├── dag_template.tpl              # DAG 템플릿 파일
│         ├── debug_router.py               # pycharm 디버그를 위한 것으로 동작 확인중 (정상 동작X)
│         ├── decorator.py                  # task 실행시 XCom/RabbitMQ/File 데이터 전달
│         ├── requirements.txt              # plugin 의 디펜던시 정의
│         ├── routes.py                     # API routing
│         └── swagger.py                    # swagger API 설정
└── requirements.txt

```

---

## 설치 및 실행

### Docker 환경에서 실행

1. Docker Compose 실행
   ```bash
   docker-compose -f docker-compose-airflow-standalone.yaml up
   ```
2. Airflow 웹 UI 접속
   - 브라우저에서 http://localhost:8082 접속
   - 기본 계정: admin / admin

---

## API 명세

1. UDF 관리

   | 메서드  | 엔드포인트             | 설명             |
   |------|-------------------|----------------|
   | POST | /api/v1/fwani/udf | UDF 업로드        |
   | GET  | /api/v1/fwani/udf | 업로드된 UDF 목록 조회 |

2. DAG 관리

   | 메서드    | 엔드포인트                      | 설명        |
   |--------|----------------------------|-----------|
   | POST   | /api/v1/fwani/dag          | DAG 생성    |
   | GET    | /api/v1/fwani/dag          | DAG 목록 조회 |
   | DELETE | /api/v1/fwani/dag/{dag_id} | 특정 DAG 삭제 |

## DAG 예제

- API를 통해 생성하는 DAG 예제

```json
{
  "dag_id": "dag_example",
  "nodes": [
    {
      "id": "task_A",
      "filename": "udf_1",
      "function": "fetch_data"
    },
    {
      "id": "task_B",
      "filename": "udf_2",
      "function": "process_data"
    },
    {
      "id": "task_C",
      "filename": "udf_3",
      "function": "store_data"
    }
  ],
  "edges": [
    {
      "from": "task_A",
      "to": "task_B"
    },
    {
      "from": "task_A",
      "to": "task_C"
    },
    {
      "from": "task_B",
      "to": "task_C"
    }
  ]
}
```

- 생성된 DAG 코드

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='dag_parallel_example',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
    },
    schedule_interval=None,
    catchup=False,
)

task_A = PythonOperator(
    task_id='task_A',
    python_callable=xcom_decorator(fetch_data),
    dag=dag
)

task_B = PythonOperator(
    task_id='task_B',
    python_callable=lambda **kwargs: xcom_decorator(process_data)(
        before_task_ids=["task_A"]),
    dag=dag
)

task_C = PythonOperator(
    task_id='task_C',
    python_callable=lambda **kwargs: xcom_decorator(store_data)(
        before_task_ids=["task_A"]),
    dag=dag
)

task_D = PythonOperator(
    task_id='task_D',
    python_callable=lambda **kwargs: xcom_decorator(finalize)(
        before_task_ids=["task_B", "task_C"]),
    dag=dag
)

task_A >> [task_B, task_C]  # 병렬 실행 표현
[task_B, task_C] >> task_D  # 병렬 Task 완료 후 단일 Task 실행
```

---

## 향후 개선 사항

- 웹 UI 추가하여 DAG을 시각적으로 편집 가능하게 개선
- DAG 실행 모니터링 API 추가
- UDF 의존성 관리 기능 추가 (requirements.txt 지원)
- RabbitMQ 데이터 전달 방식 지원 (현재는 XCom/local file 사용)
