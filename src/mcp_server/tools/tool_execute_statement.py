import time
import json

from mcp_server.context import get_connection_manager, get_allowlist, get_query_validator
from mcp_server._dataclasses.query_result import QueryResult


def execute_statement(
        connection_name: str,
        sql: str,
        database: str | None = None ) -> str:
    """Execute a write statement (INSERT/UPDATE/CREATE/ALTER). DELETE and DROP are blocked."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()
    validator = get_query_validator()

    try:
        validator.validate_no_multi_statement(sql)
        stmt_type = validator.validate_statement(sql)

        adapter = manager.get_adapter(connection_name)

        if database:
            allowlist.validate_database(connection_name, database)
            adapter.ensure_connected()
            use_sql = f"USE [{database}]" if adapter.config.driver == "sql_server" else f"USE `{database}`"
            cursor = adapter._conn.cursor()
            cursor.execute(use_sql)
            cursor.close()

        start                       = time.perf_counter()
        columns, rows, affected     = adapter.execute(sql)
        elapsed                     = (time.perf_counter() - start) * 1000

        result = QueryResult(
            success           = True,
            connection        = connection_name,
            database          = database or adapter.config.database,
            columns           = columns,
            rows              = rows,
            row_count         = affected,
            execution_time_ms = elapsed,
            message           = f"{stmt_type} executed successfully. {affected} row(s) affected.",
            statement_type    = stmt_type,
        )

        return json.dumps(result.to_dict(), indent=2, default=str)

    except Exception as e:
        result = QueryResult(
            success        = False,
            connection     = connection_name,
            database       = database or "",
            message        = f"{type(e).__name__}: {e}",
            statement_type = "STATEMENT",
        )

        return json.dumps(result.to_dict(), indent=2)
