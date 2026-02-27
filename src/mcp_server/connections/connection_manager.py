from mcp_server._dataclasses.connection_config import ConnectionConfig
from mcp_server._errors.connection_error import SqlConnectionError
from mcp_server.connections.base_adapter import BaseAdapter
from mcp_server.connections.sql_server_adapter import SqlServerAdapter
from mcp_server.connections.mysql_adapter import MySqlAdapter


class ConnectionManager:
    """Manages named database connections and adapter lifecycle."""

    DRIVER_MAP = {
        "sql_server": SqlServerAdapter,
        "mysql":      MySqlAdapter,
    }

    def __init__(self, connections_config: dict):
        self._configs:  dict[str, ConnectionConfig] = {}
        self._adapters: dict[str, BaseAdapter]      = {}

        for name, cfg in connections_config.items():
            self._configs[name] = ConnectionConfig.from_dict(name, cfg)

    def get_adapter(self, connection_name: str) -> BaseAdapter:
        """Get or create an adapter for a named connection."""

        if connection_name not in self._configs:
            raise SqlConnectionError(
                connection_name,
                f"Unknown connection. Available: {list(self._configs.keys())}",
            )

        if connection_name not in self._adapters:
            config        = self._configs[connection_name]
            adapter_class = self.DRIVER_MAP.get(config.driver)

            if adapter_class is None:
                raise SqlConnectionError(
                    connection_name,
                    f"Unsupported driver '{config.driver}'. Supported: {list(self.DRIVER_MAP.keys())}",
                )

            self._adapters[connection_name] = adapter_class(config)

        return self._adapters[connection_name]

    def list_connections(self) -> list[dict]:
        """Return summary of all configured connections."""

        return [
            {
                "name":     cfg.name,
                "driver":   cfg.driver,
                "host":     cfg.host,
                "database": cfg.database,
            }
            for cfg in self._configs.values()
        ]

    def disconnect_all(self) -> None:
        """Disconnect all active adapters."""

        for adapter in self._adapters.values():
            if adapter.is_connected:
                adapter.disconnect()

        self._adapters.clear()
