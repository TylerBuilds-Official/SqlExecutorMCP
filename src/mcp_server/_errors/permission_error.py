class SqlPermissionError(Exception):
    """Raised when a query targets a disallowed database or schema."""

    def __init__(self, connection_name: str, target: str, detail: str = ""):
        self.connection_name = connection_name
        self.target          = target
        self.detail          = detail

        super().__init__(
            f"Permission denied on '{connection_name}' for target '{target}': {detail}"
        )
