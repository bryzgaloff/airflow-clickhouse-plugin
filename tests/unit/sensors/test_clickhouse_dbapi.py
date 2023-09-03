import unittest
from unittest import mock

from airflow_clickhouse_plugin.sensors.clickhouse_dbapi import \
    ClickHouseSqlSensor


class ClickHouseSqlSensorTestCase(unittest.TestCase):
    def test_arguments(self):
        return_value = ClickHouseSqlSensor(
            task_id='test1',  # required by Airflow
            sql='SELECT 1',  # required by SqlSensor
            conn_id='test-conn-id',
            hook_params={'test_param': 'test-param-value'},
        )._get_hook()
        self._hook_cls_mock.assert_called_once_with(
            clickhouse_conn_id='test-conn-id',
            test_param='test-param-value',
        )
        self.assertIs(return_value, self._hook_cls_mock.return_value)

    def test_defaults(self):
        ClickHouseSqlSensor(
            task_id='test2',  # required by Airflow
            sql='SELECT 2',  # required by SqlSensor
            conn_id='test-conn-id',  # required by SqlSensor
        )._get_hook()
        self._hook_cls_mock.assert_called_once_with(
            clickhouse_conn_id='test-conn-id',
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
