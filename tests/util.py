from clickhouse_driver import Client

from clickhouse_plugin.hooks import clickhouse_hook


class LocalClickHouseHook(clickhouse_hook.ClickHouseHook):
    def get_conn(self) -> Client:
        return Client('localhost')