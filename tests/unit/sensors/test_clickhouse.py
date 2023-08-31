import unittest
from unittest import mock

from airflow import AirflowException

from airflow_clickhouse_plugin.sensors.clickhouse import ClickHouseSensor


class ClickHouseSensorTestCase(unittest.TestCase):
    def test_arguments(self):
        is_success_mock = mock.Mock()
        is_failure_mock = mock.Mock(return_value=False)
        execute_mock: mock.Mock = self._hook_cls_mock.return_value.execute
        return_value = ClickHouseSensor(
            task_id='test1',  # required by Airflow
            sql='SELECT 1',
            parameters=[('test-param', 1)],
            with_column_types=True,
            external_tables=[{'name': 'ext'}],
            query_id='test-query-id',
            settings={'test-setting': 1},
            types_check=True,
            columnar=True,
            clickhouse_conn_id='test-conn-id',
            database='test-database',
            is_success=is_success_mock,
            is_failure=is_failure_mock,
        ).poke(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_cls_mock.assert_called_once_with(
                clickhouse_conn_id='test-conn-id',
                database='test-database',
            )
        with self.subTest('ClickHouseHook.execute'):
            execute_mock.assert_called_once_with(
                'SELECT 1',
                [('test-param', 1)],
                True,
                [{'name': 'ext'}],
                'test-query-id',
                {'test-setting': 1},
                True,
                True,
            )
        with self.subTest('is_failure'):
            is_failure_mock.assert_called_once_with(execute_mock.return_value)
        with self.subTest('return value'):
            is_success_mock.assert_called_once_with(execute_mock.return_value)
            self.assertIs(return_value, is_success_mock.return_value)

    def test_defaults(self):
        # side_effect is for bool_mock to operate as real bool during __init__
        bool_mock = mock.Mock(side_effect=bool)
        with mock.patch('builtins.bool', bool_mock):
            operator = ClickHouseSensor(
                task_id='test2',  # required by Airflow
                sql='SELECT 2',
            )
        bool_mock.side_effect = None
        return_value = operator.poke(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_cls_mock.assert_called_once_with(
                clickhouse_conn_id='clickhouse_default',
                database=None,
            )
        with self.subTest('ClickHouseHook.execute'):
            self._hook_cls_mock.return_value.execute.assert_called_once_with(
                'SELECT 2',
                None,
                False,
                None,
                None,
                None,
                False,
                False,
            )
        with self.subTest('is_success is bool'):
            self.assertIs(return_value, bool_mock.return_value)

    def test_failure(self):
        is_failure_mock = mock.Mock(return_value=True)
        with self.assertRaisesRegexp(AirflowException, 'is_failure returned True'):
            ClickHouseSensor(
                task_id='test3',  # required by Airflow
                sql='SELECT 3',
                is_failure=is_failure_mock,
            ).poke(context={})

    def setUp(self):
        self._hook_cls_patcher = mock.patch('.'.join((
            'airflow_clickhouse_plugin.operators',
            'clickhouse.ClickHouseHook',
        )))
        self._hook_cls_mock = self._hook_cls_patcher.start()

    def tearDown(self):
        self._hook_cls_patcher.stop()


class ClickHouseSensorClassTestCase(unittest.TestCase):
    def test_template_fields(self):
        self.assertSetEqual(
            {
                '_sql',
                '_params',
                '_external_tables',
                '_query_id',
                '_settings',
                '_database',
            },
            frozenset(ClickHouseSensor.template_fields),
        )


if __name__ == '__main__':
    unittest.main()
