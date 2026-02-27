from dataclasses import dataclass


@dataclass
class AllowedTarget:
    """Defines permitted databases and schemas for a connection."""

    connection_name: str
    databases:       list[str]
    schemas:         list[str]

    @property
    def allows_all_schemas(self) -> bool:
        return "*" in self.schemas

    def is_database_allowed(self, database: str) -> bool:
        """Check if a database name is in the allowlist."""

        return database in self.databases

    def is_schema_allowed(self, schema: str) -> bool:
        """Check if a schema name is in the allowlist."""

        if self.allows_all_schemas:

            return True

        return schema in self.schemas
