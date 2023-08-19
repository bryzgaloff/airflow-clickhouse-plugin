import unittest
from unittest import mock

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import \
    ClickHouseBaseDbApiOperator


class ClickHouseBaseDbApiOperatorTestCase(unittest.TestCase):
    def test_arguments(self):
        return_value = ClickHouseBaseDbApiOperator(
            task_id='test1',  # required by Airflow
            conn_id='test-conn-id',
            database='test-database',
            hook_params={'test_param': 'test-param-value'},
        ).get_db_hook()
        self._hook_cls_mock.assert_called_once_with(
            clickhouse_conn_id='test-conn-id',
            database='test-database',
            test_param='test-param-value',
        )
        self.assertIs(return_value, self._hook_cls_mock.return_value)

    def test_defaults(self):
        ClickHouseBaseDbApiOperator(
            task_id='test2',  # required by Airflow
        ).get_db_hook()
        self._hook_cls_mock.assert_called_once_with(
            clickhouse_conn_id=None,
            database=None,
        )

    def setUp(self):
        self._hook_cls_patcher = mock.patch('.'.join((
            'airflow_clickhouse_plugin.operators',
            'clickhouse_dbapi.ClickHouseDbApiHook',
        )))
        self._hook_cls_mock = self._hook_cls_patcher.start()

    def tearDown(self):
        self._hook_cls_patcher.stop()


if __name__ == '__main__':
    unittest.main()
