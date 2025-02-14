import pydevd_pycharm
from flask import Flask

from fwani_airflow_plugin import fwani_api_bp, swagger_bp

app = Flask(__name__)

app.register_blueprint(fwani_api_bp)
app.register_blueprint(swagger_bp)

pydevd_pycharm.settrace(
    'host.docker.internal',  # PyCharm Debug 서버 호스트
    port=5678,  # PyCharm에서 설정한 포트
    stdoutToServer=True,
    stderrToServer=True,
    suspend=False
)

if __name__ == '__main__':
    print("🚀 Debug Server Running on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
