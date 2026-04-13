import unittest
from unittest import mock

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseBaseDbApiOperator,
    ClickHouseSQLExecuteQueryOperator,
)


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
            schema='test-database',
            test_param='test-param-value',
        )
        self.assertIs(return_value, self._hook_cls_mock.return_value)

    def test_defaults(self):
        ClickHouseBaseDbApiOperator(
            task_id='test2',  # required by Airflow
        ).get_db_hook()
        self._hook_cls_mock.assert_called_once_with(schema=None)

    def setUp(self):
        self._hook_cls_patcher = mock.patch('.'.join((
            'airflow_clickhouse_plugin.operators',
            'clickhouse_dbapi.ClickHouseDbApiHook',
        )))
        self._hook_cls_mock = self._hook_cls_patcher.start()

    def tearDown(self):
        self._hook_cls_patcher.stop()


class ClickHouseSQLExecuteQueryOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'sql': 'SELECT 1',
                'autocommit': True,
                'parameters': {'a': 1},
                'handler': list,
                'split_statements': True,
                'return_last': True,
                'show_return_value_in_logs': True,
                'output_processor': lambda x: x,
                'requires_result_fetch': True,
            }
            # task_id is required by BaseOperator
            ClickHouseSQLExecuteQueryOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


if __name__ == '__main__':
    unittest.main()
