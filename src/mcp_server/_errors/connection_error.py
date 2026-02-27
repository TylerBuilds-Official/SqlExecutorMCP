class SqlConnectionError(Exception):
    """Raised when a database connection fails."""

    def __init__(self, connection_name: str, detail: str = ""):
        self.connection_name = connection_name
        self.detail          = detail

        super().__init__(f"Connection '{connection_name}' failed: {detail}")
