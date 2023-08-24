import os
import unittest
from unittest import mock

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook, default_conn_name


class ClickHouseConnectionEnvironTestCaseBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._env_patcher = mock.patch.dict(os.environ)
        cls._env_patcher.start()
        os.environ.setdefault(
            f'AIRFLOW_CONN_{default_conn_name.upper()}',
            'clickhouse://localhost',
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._env_patcher.stop()
