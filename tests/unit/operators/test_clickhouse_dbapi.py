import unittest
from unittest import mock

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseBaseDbApiOperator,
    ClickHouseBranchSQLOperator,
    ClickHouseSQLCheckOperator,
    ClickHouseSQLColumnCheckOperator,
    ClickHouseSQLExecuteQueryOperator,
    ClickHouseSQLIntervalCheckOperator,
    ClickHouseSQLTableCheckOperator,
    ClickHouseSQLThresholdCheckOperator,
    ClickHouseSQLValueCheckOperator,
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
            ClickHouseSQLExecuteQueryOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLColumnCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLColumnCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'table': 'test_table',
                'column_mapping': {'col1': {'null_check': {'equal_to': 0}}},
                'partition_clause': 'id > 0',
                'accept_none': False,
            }
            ClickHouseSQLColumnCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLTableCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLTableCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'table': 'test_table',
                'checks': {'row_count_check': {'check_statement': 'COUNT(*) = 1000'}},
                'partition_clause': 'id > 0',
            }
            ClickHouseSQLTableCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'sql': 'SELECT COUNT(*) FROM test_table',
                'parameters': {'table': 'test_table'},
            }
            ClickHouseSQLCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLValueCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLValueCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'sql': 'SELECT COUNT(*) FROM test_table',
                'pass_value': 100,
                'tolerance': 0.1,
            }
            ClickHouseSQLValueCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLIntervalCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'table': 'test_table',
                'metrics_thresholds': {'count': 1.5},
                'date_filter_column': 'created_at',
                'days_back': -3,
                'ratio_formula': 'relative_diff',
                'ignore_zero': False,
            }
            ClickHouseSQLIntervalCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseSQLThresholdCheckOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.SQLThresholdCheckOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'sql': 'SELECT COUNT(*) FROM test_table',
                'min_threshold': 10,
                'max_threshold': 1000,
            }
            ClickHouseSQLThresholdCheckOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


class ClickHouseBranchSQLOperatorTestCase(unittest.TestCase):
    def test_init_arguments(self):
        with mock.patch(
            'airflow.providers.common.sql.operators.sql.BranchSQLOperator.__init__',
            return_value=None,
        ) as mock_init:
            params = {
                'sql': 'SELECT 1',
                'follow_task_ids_if_true': ['task_a', 'task_b'],
                'follow_task_ids_if_false': ['task_c'],
                'parameters': {'limit': 100},
            }
            ClickHouseBranchSQLOperator(task_id='test-task-id', **params)
            mock_init.assert_called_once()
            _, kwargs = mock_init.call_args
            for name, value in params.items():
                self.assertEqual(kwargs[name], value)


if __name__ == '__main__':
    unittest.main()
