## 📡 WebSocket 엔드포인트

- **이 API에는 아래의 WebSocket 엔드포인트가 존재합니다.**
- *WebSocket 연결은 Swagger UI 에서 직접 실행 할 수는 없으며, 별도의 WebSocket 클라이언트를 사용해 주세요.*
### `ws://localhost:5050/api/v2/ws/dag-runs-history`
- 특정 DAG의 실행 이력과 Task 상태를 실시간으로 Push 받는 WebSocket 엔드포인트입니다.

**Query Parameters:**

- `dag_id` (필수): DAG ID
- `user_id` (선택): 사용자 ID (기본값: "anonymous")

**동작 방식:**

- 연결 직후 DAG 실행 이력을 5초마다 확인 후 **변화된 값을 감지하면** 메시지를 Push 합니다.
- 이후 클라이언트가 `{"run_id": "<DAG Run ID>"}` 메시지를 보내면 해당 Run의 **Task 정보도** Push 됩니다.
- 클라이언트가 `{"run_id": null}`을 보내면 Task 스트리밍을 중단합니다.

**WebSocket 클라이언트 예시 툴:**

- [WebSocket King](https://websocketking.com/)
- Postman (WebSocket 지원)
