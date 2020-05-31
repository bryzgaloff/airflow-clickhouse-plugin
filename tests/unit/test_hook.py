import json
from typing import Any, Dict
from unittest import TestCase, mock

import airflow.models
import clickhouse_driver

from airflow_clickhouse_plugin import ClickHouseHook


class ClickHouseConnectionParamsTestCase(TestCase):
    def test_port(self):
        port = 8888
        self._connection_kwargs.update(port=str(port))
        self._test_client_initiated_with(port=port)

    def test_login(self):
        login = 'user'
        self._connection_kwargs.update(login=login)
        self._test_client_initiated_with(user=login)

    def test_password(self):
        password = 'password'
        self._connection_kwargs.update(password=password)
        self._test_client_initiated_with(password=password)

    def test_database(self):
        database = 'test-db-from-init-args'
        ClickHouseHook(database=database).get_conn()
        self._assert_client_initiated_with(database=database)

    def test_schema(self):
        schema = 'test-db-from-connection-schema'
        self._connection_kwargs.update(schema=schema)
        self._test_client_initiated_with(database=schema)

    def test_host(self):
        host = 'test-host'
        self._connection_kwargs.update(host=host)
        self._test_client_initiated_with(host)

    def test_host_missing(self):
        self._test_client_initiated_with('localhost')

    def test_extra_params(self):
        extra_params = dict(
            int_param=123,
            float_param=0.456,
            list_param=['abc', True, 12.34],
            obj_param={'a': 'b', 'c': [1, 2, 3], 'd': {'e': False}},
            string_param='value',
            true_param=True,
            false_param=False,
            none_param=None,
        )
        for key, value in extra_params.items():
            with self.subTest(key):
                self._connection_kwargs.clear()
                extra_kwarg = {key: value}
                self._connection_kwargs['extra'] = extra_kwarg
                self._test_client_initiated_with(**extra_kwarg)

    def _test_client_initiated_with(self, *args, **kwargs) -> None:
        self._client_mock.reset_mock()
        ClickHouseHook().get_conn()  # instantiate connection
        self._assert_client_initiated_with(*args, **kwargs)

    def _assert_client_initiated_with(self, *args, **kwargs) -> None:
        if not args:
            args = ('localhost',)  # host argument defaults to localhost
        args = (clickhouse_driver.Client,) + args
        self._client_mock.assert_called_once_with(*args, **kwargs)

    _connection_kwargs: Dict[str, Any] = {}
    _patched_hook_connection: mock._patch
    _patched_client: mock._patch
    _client_mock: mock.Mock

    @classmethod
    def setUpClass(cls):
        cls._patched_hook_connection = mock.patch(
            'airflow_clickhouse_plugin.ClickHouseHook.get_connection',
            lambda self, conn_id: airflow.models.Connection(**dict(
                cls._connection_kwargs,
                extra=json.dumps(cls._connection_kwargs['extra']),
            ) if 'extra' in cls._connection_kwargs else cls._connection_kwargs),
        )
        cls._patched_hook_connection.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._patched_hook_connection.__exit__(None, None, None)

    def setUp(self):
        self._connection_kwargs.clear()
        self._client_mock = mock.Mock()
        self._patched_client = \
            mock.patch('clickhouse_driver.Client.__new__', self._client_mock)
        self._patched_client.__enter__()

    def tearDown(self):
        self._patched_client.__exit__(None, None, None)


class HookLogQueryTestCase(TestCase):
    def setUp(self) -> None:
        self.hook = ClickHouseHook()

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
