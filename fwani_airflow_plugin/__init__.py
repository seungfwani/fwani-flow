from airflow.plugins_manager import AirflowPlugin

from fwani_airflow_plugin.routes import fwani_api_bp
from fwani_airflow_plugin.swagger import swagger_bp


class FwaniAirflowPlugin(AirflowPlugin):
    name = "fwani_airflow_plugin"
    flask_blueprints = [fwani_api_bp, swagger_bp]
