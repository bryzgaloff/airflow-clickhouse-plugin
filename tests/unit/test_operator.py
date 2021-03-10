import unittest
from unittest import mock

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


class ClickHouseOperatorTestCase(unittest.TestCase):
    @mock.patch(
        'airflow_clickhouse_plugin'
            '.operators.clickhouse_operator.ClickHouseHook',
    )
    def test(self, clickhouse_hook_mock: mock.MagicMock):
        sql = object()
        clickhouse_conn_id = object()
        parameters = object()
        database = object()
        op = ClickHouseOperator(
            task_id='_', sql=sql, clickhouse_conn_id=clickhouse_conn_id,
            parameters=parameters, database=database,
        )
        op.execute(context=dict())
        clickhouse_hook_mock.assert_called_once_with(
            clickhouse_conn_id=clickhouse_conn_id,
            database=database,
        )
        clickhouse_hook_mock().run.assert_called_once_with(sql, parameters)

    @mock.patch(
        'airflow_clickhouse_plugin'
            '.operators.clickhouse_operator.ClickHouseHook',
    )
    def test_defaults(self, clickhouse_hook_mock: mock.MagicMock):
        sql = 'SELECT 1'
        op = ClickHouseOperator(task_id='_', sql=sql)
        op.execute(context=dict())
        clickhouse_hook_mock.assert_called_once_with(
            clickhouse_conn_id=ClickHouseHook.default_conn_name,
            database=None,
        )
        clickhouse_hook_mock().run.assert_called_once_with(sql, None)


if __name__ == '__main__':
    unittest.main()
