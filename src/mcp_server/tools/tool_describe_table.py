import json

from mcp_server.context import get_connection_manager, get_allowlist


def describe_table(
        connection_name: str,
        database: str,
        table: str,
        schema: str | None = None ) -> str:
    """Return column metadata for a specific table."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()

    try:
        allowlist.validate_database(connection_name, database)

        if schema:
            allowlist.validate_schema(connection_name, schema)

        adapter = manager.get_adapter(connection_name)
        adapter.ensure_connected()
        columns = adapter.describe_table(database, table, schema)

        return json.dumps({
            "success":    True,
            "connection": connection_name,
            "database":   database,
            "table":      table,
            "schema":     schema,
            "columns":    columns,
            "count":      len(columns),
        }, indent=2)

    except Exception as e:

        return json.dumps({
            "success":    False,
            "connection": connection_name,
            "database":   database,
            "table":      table,
            "message":    f"{type(e).__name__}: {e}",
        }, indent=2)
