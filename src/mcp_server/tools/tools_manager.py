from mcp.server import FastMCP

from mcp_server.tools.tool_execute_query import execute_query
from mcp_server.tools.tool_execute_statement import execute_statement
from mcp_server.tools.tool_list_databases import list_databases
from mcp_server.tools.tool_list_tables import list_tables
from mcp_server.tools.tool_describe_table import describe_table
from mcp_server.tools.tool_get_schema import get_schema
from mcp_server.tools.tool_delete_statement import delete_statement
from mcp_server.tools.tool_drop_statement import drop_statement


class ToolsManager:
    """Registers all SQL executor tools with the FastMCP server."""

    def __init__(self, server: FastMCP = None):
        self.server = server

        self.tools = {
            "execute_query":     execute_query,
            "execute_statement": execute_statement,
            "list_databases":    list_databases,
            "list_tables":       list_tables,
            "describe_table":    describe_table,
            "get_schema":        get_schema,
            "delete_statement":  delete_statement,
            "drop_statement":    drop_statement,
        }

    def populate_tools(self):
        """Register all tools on the FastMCP server."""

        if self.server is None:

            return

        self.server.add_tool(
            self.tools["execute_query"],
            "execute_query",
            "Execute Query",
            "Execute a read-only SELECT query against a named connection. "
            "Returns structured JSON with column metadata, rows, row count, and timing. "
            "Params: connection_name (str), sql (str), database (str, optional).",
        )

        self.server.add_tool(
            self.tools["execute_statement"],
            "execute_statement",
            "Execute Statement",
            "Execute a write statement (INSERT, UPDATE, CREATE, ALTER, MERGE). "
            "DELETE and DROP are explicitly blocked — use the dedicated tools. "
            "Params: connection_name (str), sql (str), database (str, optional).",
        )

        self.server.add_tool(
            self.tools["list_databases"],
            "list_databases",
            "List Databases",
            "List all databases on a connection, filtered to the configured allowlist. "
            "Params: connection_name (str).",
        )

        self.server.add_tool(
            self.tools["list_tables"],
            "list_tables",
            "List Tables",
            "List tables in a database, optionally filtered by schema. "
            "Validates against the allowlist before querying. "
            "Params: connection_name (str), database (str), schema (str, optional).",
        )

        self.server.add_tool(
            self.tools["describe_table"],
            "describe_table",
            "Describe Table",
            "Return column-level metadata for a table: name, type, nullable, max_length, default, position. "
            "Params: connection_name (str), database (str), table (str), schema (str, optional).",
        )

        self.server.add_tool(
            self.tools["get_schema"],
            "get_schema",
            "Get Schema",
            "Full schema introspection: returns all tables and their columns for a database/schema. "
            "Can be large for big databases — prefer describe_table for targeted lookups. "
            "Params: connection_name (str), database (str), schema (str, optional).",
        )

        self.server.add_tool(
            self.tools["delete_statement"],
            "delete_statement",
            "Delete Statement",
            "Execute a DELETE statement. GATED: requires explicit permission. "
            "Enforces WHERE clause — bare DELETE is rejected. "
            "Params: connection_name (str), sql (str), database (str, optional).",
        )

        self.server.add_tool(
            self.tools["drop_statement"],
            "drop_statement",
            "Drop Statement",
            "Execute a DROP statement. GATED: requires explicit permission. "
            "Use with extreme caution — this is irreversible. "
            "Params: connection_name (str), sql (str), database (str, optional).",
        )
