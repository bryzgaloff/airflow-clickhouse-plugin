import unittest
from unittest import mock

from airflow.models import Connection

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook, \
    _format_query_log


class ClickHouseHookTestCase(unittest.TestCase):
    def test_arguments(self):
        queries = ['SELECT 1', 'SELECT 2']
        client_instance_mock = self._client_cls_mock.return_value
        client_instance_mock.execute.side_effect = [1, 2]
        self._get_connection_mock.return_value = Connection(
            conn_id='test-conn-id',
            host='test-host',
            port=1234,
            login='test-login',
            password='test-pass',
            schema='test-schema',
            extra='{"test_extra": "test-extra-value", "secure": "true", "verify": "false"}',
        )

        return_value = ClickHouseHook(
            clickhouse_conn_id='test-conn-id',
            database='test-database',
        ).execute(
            sql=queries,
            params=[('test-param', 1)],
            with_column_types=True,
            external_tables=[{'name': 'ext'}],  # type: ignore
            query_id='test-query-id',
            settings={'test-setting': 1},
            types_check=True,
            columnar=True,
        )

        with self.subTest('connection id'):
            self._get_connection_mock.assert_called_once_with('test-conn-id')

        with self.subTest('Client.__init__'):
            self._client_cls_mock.assert_called_once_with(
                host='test-host',
                port=1234,
                user='test-login',
                password='test-pass',
                database='test-database',
                test_extra='test-extra-value',
                secure=True,
                verify=False,
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
        client_instance_mock = self._client_cls_mock.return_value
        client_instance_mock.execute.return_value = 'test-return-value'
        self._get_connection_mock.return_value = Connection()

        return_value = ClickHouseHook().execute('SELECT 1')

        with self.subTest('connection id'):
            self._get_connection_mock \
                .assert_called_once_with('clickhouse_default')

        with self.subTest('Client.__init__'):
            self._client_cls_mock.assert_called_once_with(host='localhost')

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
        self._client_cls_patcher = mock.patch('clickhouse_driver.Client')
        self._client_cls_mock = self._client_cls_patcher.start()
        self._get_connection_patcher = \
            mock.patch.object(ClickHouseHook, 'get_connection')
        self._get_connection_mock = self._get_connection_patcher.start()

    def tearDown(self):
        self._client_cls_patcher.stop()
        self._get_connection_patcher.stop()


class ClickHouseHookLoggingTestCase(unittest.TestCase):
    def test(self):
        test_generator = (_ for _ in range(1))
        subtests = (
            # params=None
            (
                'SELECT 1', None,
                'SELECT 1',
            ),
            # params: list
            (
                'INSERT INTO test2 VALUES', [],
                'INSERT INTO test2 VALUES',
            ),
            (
                'INSERT INTO test3 VALUES', [3],
                'INSERT INTO test3 VALUES with [3]',
            ),
            (
                'INSERT INTO test4 VALUES', [val for val in range(11)],
                ''.join((
                    'INSERT INTO test4 VALUES with [',
                    ', '.join(map(str, range(10))),
                    ' … and 1 more parameters]',
                )),
            ),
            # params: tuple
            (
                'INSERT INTO test5 VALUES', (),
                'INSERT INTO test5 VALUES',
            ),
            (
                'INSERT INTO test6 VALUES', (6,),
                'INSERT INTO test6 VALUES with (6,)',
            ),
            (
                'INSERT INTO test7 VALUES', tuple(val for val in range(11)),
                ''.join((
                    'INSERT INTO test7 VALUES with (',
                    ', '.join(map(str, range(10))),
                    ' … and 1 more parameters)',
                )),
            ),
            # params: dict
            (
                'SELECT 8', {},
                'SELECT 8',
            ),
            (
                'SELECT %(param)s', {'param': 9},
                "SELECT %(param)s with {'param': 9}",
            ),
            (
                'SELECT 10', {k: k for k in range(11)},
                ''.join((
                    'SELECT 10 with {',
                    ', '.join(f'{key}: {key}' for key in range(10)),
                    ' … and 1 more parameters}',
                )),
            ),
            # params: Generator
            (
                'INSERT INTO test11 VALUES', test_generator,
                f'INSERT INTO test11 VALUES with {test_generator}',
            ),
        )
        for query, params, expected in subtests:
            with self.subTest((query, params)):
                self.assertEqual(expected, _format_query_log(query, params))


if __name__ == '__main__':
    unittest.main()
