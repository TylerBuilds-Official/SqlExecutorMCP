import json

from mcp_server.context import get_connection_manager, get_allowlist


def get_schema(
        connection_name: str,
        database: str,
        schema: str | None = None ) -> str:
    """Return full schema introspection: all tables with their columns."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()

    try:
        allowlist.validate_database(connection_name, database)

        if schema:
            allowlist.validate_schema(connection_name, schema)

        adapter = manager.get_adapter(connection_name)
        adapter.ensure_connected()
        tables  = adapter.get_tables(database, schema)

        schema_map = {}
        for tbl in tables:
            table_name = tbl["table"]
            tbl_schema = tbl.get("schema", schema)
            columns    = adapter.describe_table(database, table_name, tbl_schema)
            key        = f"{tbl_schema}.{table_name}" if tbl_schema else table_name

            schema_map[key] = {
                "schema":     tbl_schema,
                "table":      table_name,
                "type":       tbl.get("type", ""),
                "columns":    columns,
                "col_count":  len(columns),
            }

        return json.dumps({
            "success":     True,
            "connection":  connection_name,
            "database":    database,
            "schema":      schema,
            "tables":      schema_map,
            "table_count": len(schema_map),
        }, indent=2)

    except Exception as e:

        return json.dumps({
            "success":    False,
            "connection": connection_name,
            "database":   database,
            "message":    f"{type(e).__name__}: {e}",
        }, indent=2)
