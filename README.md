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
├── docs                                    # workflow 설명 문서들
├── example                                 # 예제 udf 파일
├── front                                   # Frontend 파일
├── requirements.txt
├── server                                  # 서버 코드
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
