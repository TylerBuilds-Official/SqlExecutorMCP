import time
import json

from mcp_server.context import get_connection_manager, get_allowlist, get_query_validator
from mcp_server._dataclasses.query_result import QueryResult


def execute_query(
        connection_name: str,
        sql: str,
        database: str | None = None ) -> str:
    """Execute a SELECT query and return results as structured JSON."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()
    validator = get_query_validator()

    try:
        validator.validate_no_multi_statement(sql)
        stmt_type = validator.validate_query(sql)

        adapter = manager.get_adapter(connection_name)

        if database:
            allowlist.validate_database(connection_name, database)
            adapter.ensure_connected()
            adapter._conn.cursor().execute(f"USE [{database}]" if adapter.config.driver == "sql_server" else f"USE `{database}`").close()

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
            statement_type    = stmt_type,
        )

        return json.dumps(result.to_dict(), indent=2, default=str)

    except Exception as e:
        result = QueryResult(
            success        = False,
            connection     = connection_name,
            database       = database or "",
            message        = f"{type(e).__name__}: {e}",
            statement_type = "SELECT",
        )

        return json.dumps(result.to_dict(), indent=2)
