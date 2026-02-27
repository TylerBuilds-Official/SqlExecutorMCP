class QueryValidationError(Exception):
    """Raised when a query contains a blocked statement type."""

    def __init__(self, statement_type: str, detail: str = ""):
        self.statement_type = statement_type
        self.detail         = detail

        super().__init__(
            f"Blocked statement type '{statement_type}': {detail}"
        )
