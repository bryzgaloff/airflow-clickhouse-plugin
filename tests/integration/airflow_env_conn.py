import os
import unittest
from unittest import mock

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


class ClickHouseConnectionEnvironTestCaseBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._env_patcher = mock.patch.dict(os.environ, {
            f'AIRFLOW_CONN_{ClickHouseHook.default_conn_name.upper()}':
                'clickhouse://localhost',
        })
        cls._env_patcher.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._env_patcher.stop()
