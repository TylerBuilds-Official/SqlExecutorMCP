from abc import ABC, abstractmethod

from mcp_server._dataclasses.connection_config import ConnectionConfig
from mcp_server._dataclasses.query_result import ColumnMeta


class BaseAdapter(ABC):
    """Abstract base for database adapters."""

    def __init__(self, config: ConnectionConfig):
        self.config     = config
        self._conn      = None

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def disconnect(self) -> None:
        ...

    @abstractmethod
    def execute(self, sql: str, params: list | None = None) -> tuple[list[ColumnMeta], list[dict], int]:
        """Execute SQL and return (columns, rows, affected_count)."""
        ...

    @abstractmethod
    def get_databases(self) -> list[str]:
        ...

    @abstractmethod
    def get_tables(self, database: str, schema: str | None = None) -> list[dict]:
        ...

    @abstractmethod
    def describe_table(self, database: str, table: str, schema: str | None = None) -> list[dict]:
        ...

    def ensure_connected(self) -> None:
        """Reconnect if connection is stale."""

        if self._conn is None:
            self.connect()

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
