from unittest import TestCase

from clickhouse_driver import Client

from clickhouse_plugin.hooks import clickhouse_hook


class LocalClickHouseHook(clickhouse_hook.ClickHouseHook):
    def get_conn(self) -> Client:
        return Client('localhost')


class ClientFromUrlTestCase(TestCase):
    def test_simple(self):
        hook = LocalClickHouseHook()
        res = hook.run('SHOW DATABASES')
        print(res)
