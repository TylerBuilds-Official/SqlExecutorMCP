import json

from mcp_server.context import get_connection_manager, get_allowlist


def list_tables(
        connection_name: str,
        database: str,
        schema: str | None = None ) -> str:
    """List tables in a database, optionally filtered by schema."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()

    try:
        allowlist.validate_database(connection_name, database)

        if schema:
            allowlist.validate_schema(connection_name, schema)

        adapter = manager.get_adapter(connection_name)
        adapter.ensure_connected()
        tables  = adapter.get_tables(database, schema)

        return json.dumps({
            "success":    True,
            "connection": connection_name,
            "database":   database,
            "schema":     schema,
            "tables":     tables,
            "count":      len(tables),
        }, indent=2)

    except Exception as e:

        return json.dumps({
            "success":    False,
            "connection": connection_name,
            "database":   database,
            "message":    f"{type(e).__name__}: {e}",
        }, indent=2)
