"""
tests.test_hook.py
~~~~~~~~~~~~~~~~~~~~

Test suite for the curves.py module that handles everything to do with
supply and demand curves.
"""

from unittest import TestCase

from clickhouse_driver import Client

from clickhouse_plugin.hooks import clickhouse_hook


class LocalClickhouseHook(clickhouse_hook.ClickHouseHook):

    def get_conn(self) -> Client:
        return Client({
            'host': 'localhost'
        })


class ClientFromUrlTestCase(TestCase):

    def test_simple(self):
        hook = LocalClickhouseHook()
        res = hook.run("SHOW DATABASES")
        print(res)
