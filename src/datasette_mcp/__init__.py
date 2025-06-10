"""
Datasette MCP

A Model Context Protocol server that provides read-only access to Datasette instances.
"""

__version__ = "0.6.1"

from .main import main

__all__ = ["main", "__version__"]