import unittest
from unittest import mock

from airflow_clickhouse_plugin.operators.clickhouse import \
    ClickHouseOperator


class ClickHouseOperatorTestCase(unittest.TestCase):
    def test_arguments(self):
        return_value = ClickHouseOperator(
            task_id='test1',  # required by Airflow
            sql='SELECT 1',
            params=[('test-param', 1)],
            with_column_types=True,
            external_tables=[{'name': 'ext'}],
            query_id='test-query-id',
            settings={'test-setting': 1},
            types_check=True,
            columnar=True,
            clickhouse_conn_id='test-conn-id',
            database='test-database',
        ).execute(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_mock.assert_called_once_with(
                clickhouse_conn_id='test-conn-id',
                database='test-database',
            )
        with self.subTest('ClickHouseHook.execute'):
            self._hook_mock.return_value.execute.assert_called_once_with(
                'SELECT 1',
                [('test-param', 1)],
                True,
                [{'name': 'ext'}],
                'test-query-id',
                {'test-setting': 1},
                True,
                True,
            )
        with self.subTest('return value'):
            self.assertIs(
                return_value,
                self._hook_mock.return_value.execute.return_value,
            )

    def test_defaults(self):
        ClickHouseOperator(
            task_id='test2',  # required by Airflow
            sql='SELECT 2',
        ).execute(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_mock.assert_called_once_with(
                clickhouse_conn_id='clickhouse_default',
                database=None,
            )
        with self.subTest('ClickHouseHook.execute'):
            self._hook_mock.return_value.execute.assert_called_once_with(
                'SELECT 2',
                None,
                False,
                None,
                None,
                None,
                False,
                False,
            )

    def setUp(self):
        self._hook_patcher = mock.patch('.'.join((
            'airflow_clickhouse_plugin.operators',
            'clickhouse.ClickHouseHook',
        )))
        self._hook_mock = self._hook_patcher.start()

    def tearDown(self):
        self._hook_patcher.stop()


if __name__ == '__main__':
    unittest.main()
