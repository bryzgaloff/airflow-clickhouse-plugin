import unittest
from unittest import mock

from airflow.models import Connection

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseHookTestCase(unittest.TestCase):
    def test_arguments(self):
        queries = ['SELECT 1', 'SELECT 2']
        client_instance_mock = self._client_mock.return_value
        client_instance_mock.execute.side_effect = [1, 2]
        return_value = ClickHouseHook(
            clickhouse_conn_id='test-conn-id',
            database='test-database',
        ).execute(
            sql=queries,
            params=[('test-param', 1)],
            with_column_types=True,
            external_tables=[{'name': 'ext'}],
            query_id='test-query-id',
            settings={'test-setting': 1},
            types_check=True,
            columnar=True,
        )

        with self.subTest('connection id'):
            self._get_connection_mock.assert_called_once_with('test-conn-id')

        with self.subTest('Client.__init__'):
            self._client_mock.assert_called_once_with(
                'test-host',
                port=1234,
                user='test-login',
                password='test-pass',
                database='test-database',
                test_extra='test-extra-value',
            )

        for query, mock_call \
                in zip(queries, client_instance_mock.execute.mock_calls):
            with self.subTest(f'Client.execute {query}'):
                self.assertEqual(
                    mock.call(
                        query,
                        params=[('test-param', 1)],
                        with_column_types=True,
                        external_tables=[{'name': 'ext'}],
                        query_id='test-query-id',
                        settings={'test-setting': 1},
                        types_check=True,
                        columnar=True,
                    ),
                    mock_call,
                )

        with self.subTest('Client.disconnect'):
            client_instance_mock.disconnect.assert_called_once_with()

        with self.subTest('return value'):
            self.assertEqual(2, return_value)

    def test_defaults(self):
        client_instance_mock = self._client_mock.return_value
        client_instance_mock.execute.return_value = 'test-return-value'
        return_value = ClickHouseHook().execute('SELECT 1')

        with self.subTest('connection id'):
            self._get_connection_mock \
                .assert_called_once_with('clickhouse_default')

        with self.subTest('Client.__init__'):
            self._client_mock.assert_called_once_with(
                'test-host',
                port=1234,
                user='test-login',
                password='test-pass',
                database='test-schema',
                test_extra='test-extra-value'
            )

        with self.subTest('Client.execute'):
            client_instance_mock.execute.assert_called_once_with(
                'SELECT 1',
                params=None,
                with_column_types=False,
                external_tables=None,
                query_id=None,
                settings=None,
                types_check=False,
                columnar=False,
            )

        with self.subTest('Client.disconnect'):
            client_instance_mock.disconnect.assert_called_once_with()

        with self.subTest('return value'):
            self.assertEqual('test-return-value', return_value)

    def setUp(self):
        self._client_patcher = mock.patch('clickhouse_driver.Client')
        self._client_mock = self._client_patcher.start()
        self._get_connection_patcher = \
            mock.patch.object(ClickHouseHook, 'get_connection')
        self._get_connection_mock = self._get_connection_patcher.start()
        self._get_connection_mock.return_value = Connection(
            conn_id='test-conn-id',
            host='test-host',
            port=1234,
            login='test-login',
            password='test-pass',
            schema='test-schema',
            extra='{"test_extra": "test-extra-value"}',
        )

    def tearDown(self):
        self._client_patcher.stop()
        self._get_connection_patcher.stop()


if __name__ == '__main__':
    unittest.main()
