from dataclasses import dataclass, field


@dataclass
class ColumnMeta:
    """Metadata for a single result column."""

    name:     str
    type:     str
    nullable: bool = True


@dataclass
class QueryResult:
    """Structured result from a SQL execution."""

    success:           bool
    connection:        str
    database:          str
    columns:           list[ColumnMeta]   = field(default_factory=list)
    rows:              list[dict]         = field(default_factory=list)
    row_count:         int                = 0
    execution_time_ms: float              = 0.0
    message:           str                = ""
    statement_type:    str                = ""

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""

        return {
            "success":           self.success,
            "connection":        self.connection,
            "database":          self.database,
            "columns":           [{"name": c.name, "type": c.type, "nullable": c.nullable} for c in self.columns],
            "rows":              self.rows,
            "row_count":         self.row_count,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "message":           self.message,
            "statement_type":    self.statement_type,
        }
