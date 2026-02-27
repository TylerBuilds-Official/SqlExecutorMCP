from dataclasses import dataclass, field


@dataclass
class ConnectionConfig:
    """Named database connection configuration."""

    name:                str
    driver:              str
    host:                str
    port:                int
    database:            str
    username:            str            = ""
    password:            str            = ""
    trusted_connection:  bool           = False
    driver_name:         str            = "ODBC Driver 17 for SQL Server"
    extra:               dict           = field(default_factory=dict)

    @staticmethod
    def from_dict(name: str, data: dict) -> "ConnectionConfig":
        """Build ConnectionConfig from a config dict entry."""

        known_keys = {
            "driver", "host", "port", "database",
            "username", "password", "trusted_connection", "driver_name",
        }
        extra = {k: v for k, v in data.items() if k not in known_keys}

        return ConnectionConfig(
            name               = name,
            driver             = data["driver"],
            host               = data["host"],
            port               = data.get("port", 1433 if data["driver"] == "sql_server" else 3306),
            database           = data.get("database", ""),
            username           = data.get("username", ""),
            password           = data.get("password", ""),
            trusted_connection = data.get("trusted_connection", False),
            driver_name        = data.get("driver_name", "ODBC Driver 17 for SQL Server"),
            extra              = extra,
        )
