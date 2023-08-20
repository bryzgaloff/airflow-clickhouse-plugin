import unittest
from unittest import mock

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseDbApiHookTestCase(unittest.TestCase):
    def test_definition(self):
        self.assertEqual('clickhouse_conn_id', ClickHouseDbApiHook.conn_name_attr)
        self.assertEqual('clickhouse_default', ClickHouseDbApiHook.default_conn_name)

    def test_arguments(self):
        conn_mock = self._get_connection_mock.return_value
        conn_mock.extra_dejson = {'test_extra': 'test-extra'}
        return_value = ClickHouseDbApiHook(
            clickhouse_conn_id='test-conn-id',
            schema='test-schema',
        ).get_conn()
        self._get_connection_mock.assert_called_once_with('test-conn-id')
        self._connect_mock.assert_called_once_with(
            user=conn_mock.login,
            password=conn_mock.password,
            host=conn_mock.host,
            port=conn_mock.port,
            database='test-schema',
            test_extra='test-extra',
        )
        self.assertIs(return_value, self._connect_mock.return_value)

    def test_defaults(self):
        ClickHouseDbApiHook().get_conn()
        conn_mock = self._get_connection_mock.return_value
        self._get_connection_mock.assert_called_once_with('clickhouse_default')
        self._connect_mock.assert_called_once_with(
            user=conn_mock.login,
            password=conn_mock.password,
            host=conn_mock.host,
            port=conn_mock.port,
            database=conn_mock.schema,
        )

    def setUp(self) -> None:
        self._get_connection_patcher = \
            mock.patch.object(ClickHouseDbApiHook, 'get_connection')
        self._get_connection_mock = self._get_connection_patcher.start()
        self._connect_patcher = mock.patch('clickhouse_driver.dbapi.connect')
        self._connect_mock = self._connect_patcher.start()

    def tearDown(self) -> None:
        self._get_connection_patcher.stop()
        self._connect_patcher.stop()


if __name__ == '__main__':
    unittest.main()
