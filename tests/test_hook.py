import json
from typing import Any, Dict
from unittest import TestCase, mock

import airflow.models
import clickhouse_driver.connection
import clickhouse_driver.defines
import clickhouse_driver.protocol
from clickhouse_driver.errors import ServerException, ErrorCodes

from airflow_clickhouse_plugin import ClickHouseHook
from tests.util import LocalClickHouseHook


class ClickHouseHookTestCaseCase(TestCase):
    def test_temp_table(self):
        hook = LocalClickHouseHook()
        temp_table_name = 'test_temp_table'
        result = hook.run((
            f'CREATE TEMPORARY TABLE {temp_table_name} (test_field UInt8)',
            f'INSERT INTO {temp_table_name} '
            f'SELECT number FROM system.numbers WHERE number < 5 LIMIT 5',
            f'SELECT SUM(test_field) FROM {temp_table_name}',
        ))
        self.assertListEqual([(10,)], result)
        try:
            # a new connection is created
            hook.run(f'SELECT * FROM {temp_table_name}')
        except ServerException as err:
            self.assertEqual(ErrorCodes.UNKNOWN_TABLE, err.code)
        else:
            raise AssertionError('server did not raise an error')


class ClickHouseConnectionParamsTestCase(TestCase):
    def test_plain_arguments(self):
        plain_arguments = dict(host='hst', password='pass')
        self._connection_kwargs.update(plain_arguments)
        connection = self._connection_from_hook()
        for key, value in plain_arguments.items():
            with self.subTest(key):
                self.assertEqual(value, getattr(connection, key))

    def test_user(self):
        login = 'user'
        self._connection_kwargs.update(login=login)
        self.assertEqual(login, self._connection_from_hook().user)

    def test_database(self):
        database = 'test-db-from-init-args'
        self.assertEqual(
            database,
            ClickHouseHook(database=database).get_conn().connection.database,
        )

    def test_schema(self):
        schema = 'test-db-from-connection-schema'
        self._connection_kwargs.update(schema=schema)
        self.assertEqual(schema, self._connection_from_hook().database)

    def test_port(self):
        port = 8888
        self._connection_kwargs.update(port=str(port))
        self.assertEqual(port, self._connection_from_hook().port)

    def test_host_missing(self):
        self.assertEqual('localhost', self._connection_from_hook().host)

    def test_plain_extra_params(self):
        extra_params = dict(
            connect_timeout=123,
            send_receive_timeout=456,
            sync_request_timeout=789,
        )
        self._connection_kwargs.update(extra=extra_params)
        connection = self._connection_from_hook()
        for key, value in extra_params.items():
            with self.subTest(key):
                self.assertEqual(value, getattr(connection, key))

    def test_client_name(self):
        client_name = 'test-client-name'
        self._connection_kwargs.update(extra=dict(client_name=client_name))
        self.assertEqual(
            f'{clickhouse_driver.defines.DBMS_NAME} {client_name}',
            self._connection_from_hook().client_name,
        )

    def test_compression_enabled(self):
        # to pass successfully requires: pip install clickhouse-driver[lz4]
        comp_block_size = 123456
        for compression in (True, 'lz4'):
            with self.subTest(compression):
                self._connection_kwargs.update(extra=dict(
                    compression=compression,
                    compress_block_size=comp_block_size,
                ))
                connection = self._connection_from_hook()
                self.assertEqual(
                    clickhouse_driver.protocol.Compression.ENABLED,
                    connection.compression,
                )
                self.assertEqual(comp_block_size, connection.compress_block_size)

    def test_compression_disabled(self):
        self._connection_kwargs.update(extra=dict(compression=False))
        self.assertEqual(
            clickhouse_driver.protocol.Compression.DISABLED,
            self._connection_from_hook().compression,
        )

    def test_secure(self):
        for secure in (False, True):
            with self.subTest(secure):
                self._connection_kwargs.update(extra=dict(secure=secure))
                self.assertEqual(
                    secure,
                    self._connection_from_hook().secure_socket,
                )

    def test_verify(self):
        for verify in (False, True):
            with self.subTest(verify):
                self._connection_kwargs.update(extra=dict(verify=verify))
                self.assertEqual(
                    verify,
                    self._connection_from_hook().verify_cert,
                )

    def test_ssl_options(self):
        ssl_options = dict(ssl_version='0.0', ca_certs='/a/b', ciphers='c')
        self._connection_kwargs.update(extra=ssl_options)
        for option, value in ssl_options.items():
            with self.subTest(option):
                self.assertEqual(
                    value,
                    self._connection_from_hook().ssl_options[option],
                )

    @staticmethod
    def _connection_from_hook() -> clickhouse_driver.connection.Connection:
        return ClickHouseHook().get_conn().connection

    _connection_kwargs: Dict[str, Any]
    _mocked_hook: mock._patch

    @classmethod
    def setUpClass(cls):
        cls._mocked_hook = mock.patch(
            'airflow_clickhouse_plugin.ClickHouseHook.get_connection',
            lambda self, conn_id: airflow.models.Connection(**dict(
                cls._connection_kwargs,
                extra=json.dumps(cls._connection_kwargs['extra']),
            ) if 'extra' in cls._connection_kwargs else cls._connection_kwargs),
        )
        cls._mocked_hook.__enter__()

    @classmethod
    def setUp(cls):
        cls._connection_kwargs = {}

    @classmethod
    def tearDownClass(cls) -> None:
        cls._mocked_hook.__exit__(None, None, None)


class HookLogQueryTestCase(TestCase):
    def setUp(self) -> None:
        self.hook = LocalClickHouseHook()

    def test_log_params_dict(self):
        self.assertEqual('{}', self.hook._log_params({}))
        self.assertEqual('{1: 1}', self.hook._log_params({1: 1}))
        self.assertEqual('{1: 1}', self.hook._log_params({1: 1}, limit=1))
        self.assertEqual(
            '{1: 1 … and 1 more parameters}',
            self.hook._log_params({1: 1, 2: 2}, limit=1),
        )
        self.assertEqual(
            '{1: 1, 2: 2}',
            self.hook._log_params({1: 1, 2: 2}),
        )
        self.assertEqual(
            '{0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9 …'
            ' and 1 more parameters}',
            self.hook._log_params({k: k for k in range(11)}),
        )
        self.assertEqual(
            '{0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9 …'
            ' and 10 more parameters}',
            self.hook._log_params({k: k for k in range(20)}),
        )
        self.assertEqual(
            '{0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9 …'
            ' and 10 more parameters}',
            self.hook._log_params({k: k for k in range(20)}, limit=10),
        )

    def test_log_params_generator(self):
        def gen():
            yield
        g = gen()
        self.assertEqual(str(g), self.hook._log_params(g))

    def test_log_params_tuple(self):
        self.assertEqual('()', self.hook._log_params(()))
        self.assertEqual('(1,)', self.hook._log_params((1, )))
        self.assertEqual('(1,)', self.hook._log_params((1, ), limit=1))
        self.assertEqual(
            '(1, … and 1 more parameters)',
            self.hook._log_params((1, 2), limit=1),
        )
        self.assertEqual(
            '(1, 2)',
            self.hook._log_params((1, 2)),
        )
        self.assertEqual(
            '(0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 1 more parameters)',
            self.hook._log_params(tuple(range(11))),
        )
        self.assertEqual(
            '(0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 10 more parameters)',
            self.hook._log_params(tuple(range(20))),
        )
        self.assertEqual(
            '(0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 10 more parameters)',
            self.hook._log_params(tuple(range(20)), limit=10),
        )

    def test_log_params_list(self):
        self.assertEqual('[]', self.hook._log_params([]))
        self.assertEqual('[1]', self.hook._log_params([1]))
        self.assertEqual('[1]', self.hook._log_params([1], limit=1))
        self.assertEqual(
            '[1 … and 1 more parameters]',
            self.hook._log_params([1, 2], limit=1),
        )
        self.assertEqual(
            '[1, 2]',
            self.hook._log_params([1, 2]),
        )
        self.assertEqual(
            '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 1 more parameters]',
            self.hook._log_params(list(range(11))),
        )
        self.assertEqual(
            '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 10 more parameters]',
            self.hook._log_params(list(range(20))),
        )
        self.assertEqual(
            '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9 … and 10 more parameters]',
            self.hook._log_params(list(range(20)), limit=10),
        )

    def test_log_query(self):
        _ = self.hook.log  # to initialize .log property
        with mock.patch.object(self.hook, '_log') as patched:
            self.hook._log_query('SELECT 1', {})
            patched.info.assert_called_with('%s%s', 'SELECT 1', '')
            self.hook._log_query('SELECT 1', {1: 1})
            patched.info.assert_called_with('%s%s', 'SELECT 1', ' with {1: 1}')
            self.hook._log_query('SELECT 1', [1])
            patched.info.assert_called_with('%s%s', 'SELECT 1', ' with [1]')


class HookGetAsPandasTestCase(TestCase):
    def test_get_pandas_df(self):
        import pandas as pd

        hook = LocalClickHouseHook()
        for sql, expected in (
            (
                '''
                SELECT
                    number,
                    concat('result: ', toString(number + number)) AS n_sum
                FROM system.numbers
                WHERE number < 4
                LIMIT 3
                ''',
                pd.DataFrame.from_dict({
                    'number': (0, 1, 2),
                    'n_sum': ('result: 0', 'result: 2', 'result: 4'),
                })
            ),
            # empty df
            (
                '''
                SELECT
                    number,
                    concat('result: ', toString(number + number)) AS n_sum
                FROM (
                    SELECT number
                    FROM system.numbers
                    WHERE number < 4
                    LIMIT 3
                )
                WHERE number > 4
                ''',
                pd.DataFrame(columns=['number', 'n_sum'])
            )
        ):
            df = hook.get_pandas_df(sql)
            self.assertListEqual(list(df.columns), list(expected.columns))
            self.assertListEqual(
                df.to_dict('records'),
                expected.to_dict('records'),
            )
