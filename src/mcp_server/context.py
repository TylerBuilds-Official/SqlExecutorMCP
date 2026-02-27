import json
from pathlib import Path

from mcp_server.connections.connection_manager import ConnectionManager
from mcp_server.security.allowlist import Allowlist
from mcp_server.security.query_validator import QueryValidator

_config_path = Path(__file__).resolve().parent.parent.parent / "config.json"

_connection_manager: ConnectionManager | None = None
_allowlist:          Allowlist | None          = None
_query_validator:    QueryValidator | None     = None


def _load_config() -> dict:
    """Load config.json from project root."""

    if not _config_path.exists():
        raise FileNotFoundError(
            f"config.json not found at {_config_path}. "
            f"Copy config.template.json to config.json and fill in your values."
        )

    with open(_config_path, "r") as f:

        return json.load(f)


def get_connection_manager() -> ConnectionManager:
    """Return the shared ConnectionManager, initializing on first call."""

    global _connection_manager

    if _connection_manager is None:
        config              = _load_config()
        _connection_manager = ConnectionManager(config["connections"])

    return _connection_manager


def get_allowlist() -> Allowlist:
    """Return the shared Allowlist, initializing on first call."""

    global _allowlist

    if _allowlist is None:
        config     = _load_config()
        _allowlist = Allowlist(config["allowlist"])

    return _allowlist


def get_query_validator() -> QueryValidator:
    """Return the shared QueryValidator."""

    global _query_validator

    if _query_validator is None:
        _query_validator = QueryValidator()

    return _query_validator
