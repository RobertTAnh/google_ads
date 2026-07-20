"""
Launcher for Claude Desktop / Antigravity — does not rely on cwd.

Usage in claude_desktop_config.json:
  "command": "<path-to-python.exe>",
  "args": ["D:\\\\1 Code App\\\\gg ads API\\\\run_mcp_stdio.py"]
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from mcp_server.server import main

if __name__ == "__main__":
    main()
