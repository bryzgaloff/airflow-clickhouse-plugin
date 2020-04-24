from unittest import TestCase, mock

import pandas as pd
from clickhouse_driver.errors import ServerException, ErrorCodes

from tests.util import LocalClickHouseHook


class ClientFromUrlTestCase(TestCase):
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
        hook = LocalClickHouseHook()
        for sql, expected in (
            (
                """
                SELECT number, concat('result: ', toString(number + number)) AS n_sum
                 FROM system.numbers
                WHERE number < 4 LIMIT 3
                """,
                pd.DataFrame.from_dict({
                    'number': (0, 1, 2),
                    'n_sum': ('result: 0', 'result: 2', 'result: 4'),
                })
            ),
            # empty df
            (
                """
                SELECT number, concat('result: ', toString(number + number)) AS n_sum
                 FROM (SELECT number FROM system.numbers WHERE number < 4 LIMIT 3)
                WHERE number > 4
                """,
                pd.DataFrame(columns=['number', 'n_sum'])
            )
        ):
            df = hook.get_pandas_df(sql)
            self.assertListEqual(list(df.columns), list(expected.columns))
            self.assertListEqual(df.to_dict('records'), expected.to_dict('records'))
