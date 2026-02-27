from mcp_server._dataclasses.allowed_target import AllowedTarget
from mcp_server._errors.permission_error import SqlPermissionError


class Allowlist:
    """Validates database/schema targets against configured allowlist."""

    def __init__(self, allowlist_config: dict):
        self._targets: dict[str, AllowedTarget] = {}

        for conn_name, rules in allowlist_config.items():
            self._targets[conn_name] = AllowedTarget(
                connection_name = conn_name,
                databases       = rules.get("databases", []),
                schemas         = rules.get("schemas", ["*"]),
            )

    def validate_database(self, connection_name: str, database: str) -> None:
        """Raise SqlPermissionError if database is not allowed."""

        target = self._targets.get(connection_name)

        if target is None:
            raise SqlPermissionError(
                connection_name, database,
                "No allowlist configured for this connection.",
            )

        if not target.is_database_allowed(database):
            raise SqlPermissionError(
                connection_name, database,
                f"Database not in allowlist. Allowed: {target.databases}",
            )

    def validate_schema(self, connection_name: str, schema: str) -> None:
        """Raise SqlPermissionError if schema is not allowed."""

        target = self._targets.get(connection_name)

        if target is None:
            raise SqlPermissionError(
                connection_name, schema,
                "No allowlist configured for this connection.",
            )

        if not target.is_schema_allowed(schema):
            raise SqlPermissionError(
                connection_name, schema,
                f"Schema not in allowlist. Allowed: {target.schemas}",
            )

    def get_allowed_databases(self, connection_name: str) -> list[str]:
        """Return the allowed databases for a connection."""

        target = self._targets.get(connection_name)

        if target is None:

            return []

        return target.databases

    def get_allowed_schemas(self, connection_name: str) -> list[str]:
        """Return the allowed schemas for a connection."""

        target = self._targets.get(connection_name)

        if target is None:

            return []

        return target.schemas
