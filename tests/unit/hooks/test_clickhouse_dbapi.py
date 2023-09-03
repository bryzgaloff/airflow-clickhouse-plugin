import unittest
from unittest import mock

from airflow.models import Connection

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseDbApiHookTestCase(unittest.TestCase):
    def test_definition(self):
        self.assertEqual('clickhouse_conn_id', ClickHouseDbApiHook.conn_name_attr)
        self.assertEqual('clickhouse_default', ClickHouseDbApiHook.default_conn_name)

    def test_arguments(self):
        self._get_connection_mock.return_value = Connection(
            conn_id='test-conn-id',
            host='test-host',
            port=1234,
            login='test-login',
            password='test-pass',
            schema='test-schema',
            extra='{"test_extra": "test-extra"}',
        )
        return_value = ClickHouseDbApiHook(
            clickhouse_conn_id='test-conn-id',
            schema='test-schema',
        ).get_conn()
        self._get_connection_mock.assert_called_once_with('test-conn-id')
        self._connect_mock.assert_called_once_with(
            user='test-login',
            password='test-pass',
            host='test-host',
            port=1234,
            database='test-schema',
            test_extra='test-extra',
        )
        self.assertIs(return_value, self._connect_mock.return_value)

    def test_defaults(self):
        self._get_connection_mock.return_value = Connection()
        ClickHouseDbApiHook().get_conn()
        self._get_connection_mock.assert_called_once_with('clickhouse_default')
        self._connect_mock.assert_called_once_with(host='localhost')

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
