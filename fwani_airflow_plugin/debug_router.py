import os

import pydevd_pycharm
import socket
import threading
from flask import Flask

from fwani_airflow_plugin import fwani_api_bp, swagger_bp

app = Flask(__name__)

app.register_blueprint(fwani_api_bp)
app.register_blueprint(swagger_bp)

# PyCharm Debug Server 연결
PYCHARM_HOST = "host.docker.internal"

# Linux 환경에서는 Docker Gateway IP로 변경
if socket.gethostname() == "airflow-standalone":
    PYCHARM_HOST = "172.17.0.1"  # `docker network inspect bridge` 결과에 따라 변경 가능

# ✅ Redis 기반의 Rate Limiting 설정
# limiter = Limiter(
#     key_func=get_remote_address,
#     storage_uri="redis://redis:6379/0"  # Redis 사용
# )

def start_debugger():
    # PyCharm Debug Server 연결
    try:
        pydevd_pycharm.settrace(
            PYCHARM_HOST,  # PyCharm Debug 서버 호스트
            port=5678,  # PyCharm에서 설정한 포트
            stdoutToServer=True,
            stderrToServer=True,
        )
    except Exception as e:
        print(f"🚨 Debug Server 연결 실패: {e}")


# Debug 서버를 백그라운드에서 실행
debug_thread = threading.Thread(target=start_debugger, daemon=True)
debug_thread.start()

# 🔥 환경 변수를 확인하여 Debug 모드 활성화 여부 결정
# if os.getenv("PYCHARM_DEBUG_ENABLED", "False").lower() == "true":
#     try:
#         pydevd_pycharm.settrace(
#             "host.docker.internal",  # PyCharm Debug Server의 호스트
#             port=5678,  # PyCharm에서 설정한 포트
#             stdoutToServer=True,
#             stderrToServer=True,
#             suspend=True  # True면 실행될 때 바로 멈춤
#         )
#         print("✅ PyCharm Debug 연결됨")
#     except Exception as e:
#         print(f"🚨 Debug Server 연결 실패: {e}")
# else:
#     print("🛑 PyCharm Debug 비활성화됨")

if __name__ == '__main__':
    print("🚀 Debug Server Running on http://0.0.0.0:5000")
    # Flask 실행
    app.run(host="0.0.0.0", port=5000, debug=True)
