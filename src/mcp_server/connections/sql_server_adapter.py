import pyodbc

from mcp_server._dataclasses.connection_config import ConnectionConfig
from mcp_server._dataclasses.query_result import ColumnMeta
from mcp_server._errors.connection_error import SqlConnectionError
from mcp_server.connections.base_adapter import BaseAdapter


class SqlServerAdapter(BaseAdapter):
    """Adapter for Microsoft SQL Server via pyodbc."""

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)

    def connect(self) -> None:
        """Establish a pyodbc connection."""

        try:
            if self.config.trusted_connection:
                conn_str = (
                    f"DRIVER={{{self.config.driver_name}}};"
                    f"SERVER={self.config.host},{self.config.port};"
                    f"DATABASE={self.config.database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={{{self.config.driver_name}}};"
                    f"SERVER={self.config.host},{self.config.port};"
                    f"DATABASE={self.config.database};"
                    f"UID={self.config.username};"
                    f"PWD={self.config.password};"
                )

            self._conn = pyodbc.connect(conn_str, timeout=10)
        except pyodbc.Error as e:
            raise SqlConnectionError(self.config.name, str(e))

    def disconnect(self) -> None:
        """Close the pyodbc connection."""

        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def execute(self, sql: str, params: list | None = None) -> tuple[list[ColumnMeta], list[dict], int]:
        """Execute SQL and return structured results."""

        self.ensure_connected()
        cursor = self._conn.cursor()

        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            columns  = []
            rows     = []
            affected = cursor.rowcount

            if cursor.description:
                columns = [
                    ColumnMeta(
                        name     = col[0],
                        type     = col[1].__name__ if hasattr(col[1], "__name__") else str(col[1]),
                        nullable = col[6] if len(col) > 6 else True,
                    )
                    for col in cursor.description
                ]

                raw_rows = cursor.fetchall()
                col_names = [c.name for c in columns]
                rows = [
                    {col_names[i]: self._serialize_value(row[i]) for i in range(len(col_names))}
                    for row in raw_rows
                ]
                affected = len(rows)

            self._conn.commit()

            return columns, rows, affected

        except pyodbc.Error as e:
            self._conn.rollback()
            raise
        finally:
            cursor.close()

    def get_databases(self) -> list[str]:
        """List all databases on the server."""

        self.ensure_connected()
        cursor = self._conn.cursor()
        cursor.execute("SELECT name FROM sys.databases ORDER BY name")
        results = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return results

    def get_tables(self, database: str, schema: str | None = None) -> list[dict]:
        """List tables in a database, optionally filtered by schema."""

        self.ensure_connected()
        cursor = self._conn.cursor()

        sql = (
            f"SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
            f"FROM [{database}].INFORMATION_SCHEMA.TABLES "
        )
        params = []

        if schema:
            sql += "WHERE TABLE_SCHEMA = ? "
            params.append(schema)

        sql += "ORDER BY TABLE_SCHEMA, TABLE_NAME"

        cursor.execute(sql, params) if params else cursor.execute(sql)
        results = [
            {"schema": row[0], "table": row[1], "type": row[2]}
            for row in cursor.fetchall()
        ]
        cursor.close()

        return results

    def describe_table(self, database: str, table: str, schema: str | None = None) -> list[dict]:
        """Return column metadata for a table."""

        self.ensure_connected()
        cursor = self._conn.cursor()

        sql = (
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
            f"CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, ORDINAL_POSITION "
            f"FROM [{database}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_NAME = ? "
        )
        params = [table]

        if schema:
            sql += "AND TABLE_SCHEMA = ? "
            params.append(schema)

        sql += "ORDER BY ORDINAL_POSITION"

        cursor.execute(sql, params)
        results = [
            {
                "column":     row[0],
                "type":       row[1],
                "nullable":   row[2] == "YES",
                "max_length": row[3],
                "default":    row[4],
                "position":   row[5],
            }
            for row in cursor.fetchall()
        ]
        cursor.close()

        return results

    @staticmethod
    def _serialize_value(value) -> object:
        """Coerce non-serializable types to strings."""

        if value is None:

            return None

        if isinstance(value, (int, float, str, bool)):

            return value

        return str(value)
