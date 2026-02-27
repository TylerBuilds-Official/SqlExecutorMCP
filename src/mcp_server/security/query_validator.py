import re
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DML, DDL

from mcp_server._errors.query_validation_error import QueryValidationError


class QueryValidator:
    """Validates SQL statements against safety rules."""

    DESTRUCTIVE_TYPES = {"DELETE", "DROP", "TRUNCATE"}

    ALWAYS_BLOCKED = {"TRUNCATE"}

    ALLOWED_FOR_QUERY   = {"SELECT"}
    ALLOWED_FOR_STMT    = {"INSERT", "UPDATE", "CREATE", "ALTER", "MERGE"}
    ALLOWED_FOR_DELETE  = {"DELETE"}
    ALLOWED_FOR_DROP    = {"DROP"}

    def detect_statement_type(self, sql: str) -> str:
        """Parse SQL and return the primary statement type."""

        parsed = sqlparse.parse(sql.strip())

        if not parsed:

            return "UNKNOWN"

        stmt      = parsed[0]
        stmt_type = stmt.get_type()

        if stmt_type:

            return stmt_type.upper()

        return self._fallback_detect(sql)

    def validate_query(self, sql: str) -> str:
        """Validate SQL is a SELECT. Returns the statement type."""

        stmt_type = self.detect_statement_type(sql)

        if stmt_type not in self.ALLOWED_FOR_QUERY:
            raise QueryValidationError(
                stmt_type,
                f"execute_query only allows SELECT statements. Got: {stmt_type}",
            )

        return stmt_type

    def validate_statement(self, sql: str) -> str:
        """Validate SQL is a safe write statement (no DELETE/DROP/TRUNCATE)."""

        stmt_type = self.detect_statement_type(sql)

        if stmt_type in self.DESTRUCTIVE_TYPES:
            raise QueryValidationError(
                stmt_type,
                f"Destructive statement '{stmt_type}' is blocked in execute_statement. "
                f"Use the dedicated delete_statement or drop_statement tools.",
            )

        if stmt_type not in self.ALLOWED_FOR_STMT and stmt_type not in self.ALLOWED_FOR_QUERY:
            raise QueryValidationError(
                stmt_type,
                f"Statement type '{stmt_type}' is not allowed in execute_statement.",
            )

        return stmt_type

    def validate_delete(self, sql: str) -> str:
        """Validate SQL is a DELETE statement specifically."""

        stmt_type = self.detect_statement_type(sql)

        if stmt_type not in self.ALLOWED_FOR_DELETE:
            raise QueryValidationError(
                stmt_type,
                f"delete_statement only allows DELETE. Got: {stmt_type}",
            )

        if not self._has_where_clause(sql):
            raise QueryValidationError(
                stmt_type,
                "DELETE without WHERE clause is not allowed. Add a WHERE clause.",
            )

        return stmt_type

    def validate_drop(self, sql: str) -> str:
        """Validate SQL is a DROP statement specifically."""

        stmt_type = self.detect_statement_type(sql)

        if stmt_type not in self.ALLOWED_FOR_DROP:
            raise QueryValidationError(
                stmt_type,
                f"drop_statement only allows DROP. Got: {stmt_type}",
            )

        return stmt_type

    def validate_no_multi_statement(self, sql: str) -> None:
        """Reject SQL containing multiple statements (injection prevention)."""

        parsed = sqlparse.parse(sql.strip())
        real   = [s for s in parsed if s.ttype is not sqlparse.tokens.Whitespace and str(s).strip()]

        if len(real) > 1:
            raise QueryValidationError(
                "MULTI",
                "Multiple statements detected. Submit one statement at a time.",
            )

    @staticmethod
    def _has_where_clause(sql: str) -> bool:
        """Check if a SQL statement contains a WHERE clause."""

        return bool(re.search(r'\bWHERE\b', sql, re.IGNORECASE))

    @staticmethod
    def _fallback_detect(sql: str) -> str:
        """Regex fallback when sqlparse can't determine type."""

        upper = sql.strip().upper()

        for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "MERGE"]:
            if upper.startswith(keyword):

                return keyword

        return "UNKNOWN"
