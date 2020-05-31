import os
import unittest

from airflow_clickhouse_plugin.hooks import ClickHouseHook


class ClickHouseConnectionEnvVarTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn_var = f'AIRFLOW_CONN_{ClickHouseHook._DEFAULT_CONN_ID.upper()}'
        os.environ.setdefault(conn_var, 'clickhouse://localhost')
