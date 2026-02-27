import sys
from pathlib import Path

_src_dir = Path(__file__).resolve().parent.parent
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

from mcp.server import FastMCP
from mcp_server.tools.tools_manager import ToolsManager

server = FastMCP("SQL Executor MCP Server")
tools  = ToolsManager(server)
tools.populate_tools()


def run():
    server.run()


if __name__ == "__main__":
    run()
