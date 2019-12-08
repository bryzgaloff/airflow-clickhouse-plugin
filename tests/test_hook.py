from unittest import TestCase

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException, ErrorCodes

from clickhouse_plugin.hooks import clickhouse_hook


class LocalClickHouseHook(clickhouse_hook.ClickHouseHook):
    def get_conn(self) -> Client:
        return Client('localhost')


class ClientFromUrlTestCase(TestCase):
    def test_simple(self):
        hook = LocalClickHouseHook()
        res = hook.run('SHOW DATABASES')
        print(res)

    def test_temp_table(self):
        hook = LocalClickHouseHook()
        temp_table_name = 'test_temp_table'
        result = hook.run(f'''
            CREATE TEMPORARY TABLE {temp_table_name} (test_field UInt8);
            INSERT INTO {temp_table_name} VALUES (1,), (2,), (3,);
            SELECT SUM(test_field) FROM {temp_table_name}
        '''.split(';'))
        self.assertListEqual([(6,)], result)
        try:
            hook.run(f'SELECT * FROM {temp_table_name}')
        except ServerException as err:
            self.assertEqual(ErrorCodes.UNKNOWN_TABLE, err.code)
        else:
            raise AssertionError('server did not raise an error')
