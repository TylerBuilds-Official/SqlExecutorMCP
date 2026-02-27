import mysql.connector

from mcp_server._dataclasses.connection_config import ConnectionConfig
from mcp_server._dataclasses.query_result import ColumnMeta
from mcp_server._errors.connection_error import SqlConnectionError
from mcp_server.connections.base_adapter import BaseAdapter


class MySqlAdapter(BaseAdapter):
    """Adapter for MySQL via mysql-connector-python."""

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)

    def connect(self) -> None:
        """Establish a MySQL connection."""

        try:
            self._conn = mysql.connector.connect(
                host     = self.config.host,
                port     = self.config.port,
                database = self.config.database,
                user     = self.config.username,
                password = self.config.password,
            )
        except mysql.connector.Error as e:
            raise SqlConnectionError(self.config.name, str(e))

    def disconnect(self) -> None:
        """Close the MySQL connection."""

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
                        type     = self._mysql_type_name(col[1]),
                        nullable = col[6] if len(col) > 6 else True,
                    )
                    for col in cursor.description
                ]

                raw_rows  = cursor.fetchall()
                col_names = [c.name for c in columns]
                rows = [
                    {col_names[i]: self._serialize_value(row[i]) for i in range(len(col_names))}
                    for row in raw_rows
                ]
                affected = len(rows)

            self._conn.commit()

            return columns, rows, affected

        except mysql.connector.Error as e:
            self._conn.rollback()
            raise
        finally:
            cursor.close()

    def get_databases(self) -> list[str]:
        """List all databases on the server."""

        self.ensure_connected()
        cursor = self._conn.cursor()
        cursor.execute("SHOW DATABASES")
        results = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return results

    def get_tables(self, database: str, schema: str | None = None) -> list[dict]:
        """List tables in a database."""

        self.ensure_connected()
        cursor = self._conn.cursor()

        cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s "
            "ORDER BY TABLE_NAME",
            [database],
        )
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

        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
            "CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, ORDINAL_POSITION "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            [database, table],
        )
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
    def _mysql_type_name(type_code) -> str:
        """Map mysql.connector type codes to readable names."""

        type_map = {
            mysql.connector.FieldType.TINY:       "TINYINT",
            mysql.connector.FieldType.SHORT:      "SMALLINT",
            mysql.connector.FieldType.LONG:       "INT",
            mysql.connector.FieldType.LONGLONG:   "BIGINT",
            mysql.connector.FieldType.FLOAT:      "FLOAT",
            mysql.connector.FieldType.DOUBLE:     "DOUBLE",
            mysql.connector.FieldType.DECIMAL:    "DECIMAL",
            mysql.connector.FieldType.NEWDECIMAL: "DECIMAL",
            mysql.connector.FieldType.STRING:     "CHAR",
            mysql.connector.FieldType.VAR_STRING: "VARCHAR",
            mysql.connector.FieldType.BLOB:       "BLOB",
            mysql.connector.FieldType.DATE:       "DATE",
            mysql.connector.FieldType.DATETIME:   "DATETIME",
            mysql.connector.FieldType.TIMESTAMP:  "TIMESTAMP",
        }

        return type_map.get(type_code, str(type_code))

    @staticmethod
    def _serialize_value(value) -> object:
        """Coerce non-serializable types to strings."""

        if value is None:

            return None

        if isinstance(value, (int, float, str, bool)):

            return value

        return str(value)
