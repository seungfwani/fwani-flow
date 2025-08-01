import logging
from typing import Generator
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from config import Config

logger = logging.getLogger()


class AirflowClient:
    def __init__(self, host: str, port: int, username: str, password: str, pool_maxsize=50, retries=3):
        self.base_url = f"http://{host}:{port}/api/v1/"
        self.username = username
        self.password = password
        self.pool_maxsize = pool_maxsize
        self.retries = retries
        self._create_session()

    def _create_session(self):
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({"Content-Type": "application/json"})

        retry_strategy = Retry(
            total=self.retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PATCH", "DELETE"],
            backoff_factor=0.5
        )
        adapter = HTTPAdapter(
            pool_connections=self.pool_maxsize,
            pool_maxsize=self.pool_maxsize,
            max_retries=retry_strategy
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _request_with_reconnect(self, method, url, **kwargs):
        try:
            logger.info(f"Requesting {method}: {url}")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except ConnectionError:
            # 세션 재생성 후 재시도
            logger.warning("[AirflowClient] Connection lost. Reconnecting...")
            self.session.close()
            self._create_session()
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def _make_url(self, endpoint: str):
        return urljoin(self.base_url, endpoint.lstrip("/"))

    def get(self, endpoint, params=None):
        url = self._make_url(endpoint)
        response = self._request_with_reconnect("GET", url, params=params)
        return response.json()

    def get_content(self, endpoint, params=None):
        url = self._make_url(endpoint)
        response = self._request_with_reconnect("GET", url, params=params)
        return response.content

    def post(self, endpoint, json_data=None):
        url = self._make_url(endpoint)
        response = self._request_with_reconnect("POST", url, data=json_data)
        return response.json()

    def patch(self, endpoint, json_data=None):
        url = self._make_url(endpoint)
        response = self._request_with_reconnect("PATCH", url, data=json_data)
        return response.json()

    def delete(self, endpoint):
        url = self._make_url(endpoint)
        response = self._request_with_reconnect("DELETE", url)
        return response.status_code == 204 or response.json()

    @staticmethod
    def _check_response(response):
        if not response.ok:
            raise Exception(f"Airflow API call failed, {response.status_code}: {response.text}")


def get_airflow_client() -> Generator[AirflowClient, None, None]:
    logger.info("Connecting to airflow server...")
    _airflow = AirflowClient(
        host=Config.AIRFLOW_HOST,
        port=Config.AIRFLOW_PORT,
        username=Config.AIRFLOW_USER,
        password=Config.AIRFLOW_PASSWORD,
    )
    try:
        yield _airflow
    finally:
        _airflow.session.close()
