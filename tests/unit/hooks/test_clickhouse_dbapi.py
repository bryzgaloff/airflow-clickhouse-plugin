import unittest

import clickhouse_driver
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook
from airflow_clickhouse_plugin.hooks.clickhouse import BaseClickHouseHook


class ClickHouseDbApiHookTestCase(unittest.TestCase):
    def test_definition(self):
        self.assertEqual(
            (ClickHouseDbApiHook, BaseClickHouseHook, DbApiHook),
            ClickHouseDbApiHook.__mro__[:3],
        )
        self.assertIs(ClickHouseDbApiHook.connector, clickhouse_driver.dbapi)
        self.assertEqual('clickhouse_default', ClickHouseDbApiHook.default_conn_name)


if __name__ == '__main__':
    unittest.main()
