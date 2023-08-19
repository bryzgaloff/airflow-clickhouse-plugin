import os
import unittest

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook


class ClickHouseConnectionEnvVarTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn_var = f'AIRFLOW_CONN_{ClickHouseDbApiHook.default_conn_name.upper()}'
        os.environ.setdefault(conn_var, 'clickhouse://localhost')
