import json

from mcp_server.context import get_connection_manager, get_allowlist


def list_databases(connection_name: str) -> str:
    """List databases on a connection, filtered to allowlist."""

    manager   = get_connection_manager()
    allowlist = get_allowlist()

    try:
        adapter       = manager.get_adapter(connection_name)
        adapter.ensure_connected()
        all_dbs       = adapter.get_databases()
        allowed_dbs   = allowlist.get_allowed_databases(connection_name)
        filtered      = [db for db in all_dbs if db in allowed_dbs] if allowed_dbs else all_dbs

        return json.dumps({
            "success":    True,
            "connection": connection_name,
            "databases":  filtered,
            "count":      len(filtered),
        }, indent=2)

    except Exception as e:

        return json.dumps({
            "success":    False,
            "connection": connection_name,
            "message":    f"{type(e).__name__}: {e}",
        }, indent=2)
