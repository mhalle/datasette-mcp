#!/usr/bin/env python3
"""
Datasette MCP Server

A Model Context Protocol server that provides read-only access to Datasette instances.
"""

__version__ = "0.5.0"
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "fastmcp>=0.2.0",
#     "httpx>=0.27.0",
#     "pyyaml>=6.0",
# ]
# ///

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode, quote, urljoin
import argparse
import logging

import httpx
from fastmcp import FastMCP, Context

from .config import (
    build_config_from_cli, 
    build_instructions, 
    derive_id_from_url,
    get_instance_config, 
    load_config, 
    validate_config
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global configuration
Config: Dict[str, Any] = {}

# Request throttling state
_last_request_time: Dict[str, float] = {}  # Track last request time per instance


async def apply_courtesy_delay(instance_name: str) -> None:
    """Apply courtesy delay between requests to the same instance."""
    global _last_request_time
    
    # Get courtesy delay setting (default 0.5 seconds)
    courtesy_delay = Config.get('courtesy_delay_seconds', 0.5)
    
    # Skip if disabled (set to 0 or negative)
    if courtesy_delay <= 0:
        return
    
    current_time = time.time()
    last_time = _last_request_time.get(instance_name, 0)
    
    # Calculate how long to wait
    time_since_last = current_time - last_time
    if time_since_last < courtesy_delay:
        sleep_time = courtesy_delay - time_since_last
        logger.debug(f"Applying courtesy delay of {sleep_time:.2f}s for instance '{instance_name}'")
        await asyncio.sleep(sleep_time)
    
    # Update last request time
    _last_request_time[instance_name] = time.time()

async def make_datasette_request(url: str, operation: str, instance_name: str) -> Dict[str, Any]:
    """Make HTTP request to Datasette API with consistent error handling."""
    # Apply courtesy delay before making request
    await apply_courtesy_delay(instance_name)
    
    # Get instance configuration for headers
    instance_config = get_instance_config(Config, instance_name)
    headers = instance_config['headers']
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            
            # Handle 400 errors (bad requests) by raising exception with detailed error
            if response.status_code == 400:
                try:
                    error_data = response.json()
                    error_message = error_data.get("error", str(response.text))
                    # Include additional error context if available
                    if isinstance(error_data, dict) and len(error_data) > 1:
                        error_details = {k: v for k, v in error_data.items() if k != "error"}
                        if error_details:
                            error_message += f" Details: {error_details}"
                except:
                    error_message = str(response.text)
                
                raise ValueError(f"Datasette API error (400): {error_message}")
            
            response.raise_for_status()
            return response.json()
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error in {operation}: {e}")
        raise RuntimeError(f"HTTP {e.response.status_code} error in {operation}: {e.response.text}")
    except ValueError:
        # Re-raise ValueError (from 400 handling above)
        raise
    except Exception as e:
        logger.error(f"Error in {operation}: {e}")
        raise RuntimeError(f"Error in {operation}: {str(e)}")

    



# URL Builder Functions

def build_url_with_params(base_url: str, params: List[tuple]) -> str:
    """Build URL with query parameters."""
    if not params:
        return base_url
    return f"{base_url}?{urlencode(params)}"

def safe_url_join(base_url: str, *paths: str) -> str:
    """Safely join base URL with path components, handling trailing slashes properly."""
    # Ensure base_url ends with exactly one slash for urljoin to work correctly
    if not base_url.endswith('/'):
        base_url += '/'
    
    # Join all path components
    url = base_url
    for path in paths:
        # Remove leading slash from path to avoid urljoin treating it as absolute
        path = path.lstrip('/')
        url = urljoin(url, path)
        # Ensure intermediate URLs end with slash for proper joining
        if not url.endswith('/') and paths.index(path) < len(paths) - 1:
            url += '/'
    
    return url

def build_sql_query_url(
    base_url: str,
    database: str,
    sql: str,
    shape: Optional[str] = None,
    json_columns: Optional[List[str]] = None,
    trace: bool = False,
    timelimit: Optional[int] = None,
    size: Optional[int] = None,
    next_token: Optional[str] = None
) -> str:
    """Build URL for executing custom SQL query."""
    url = safe_url_join(base_url, f"{quote(database)}.json")
    params = [("sql", sql)]
    
    if shape is not None:
        params.append(("_shape", shape))
    if json_columns:
        params.append(("_json", ",".join(json_columns)))
    if trace:
        params.append(("_trace", "on"))
    if timelimit is not None:
        params.append(("_timelimit", str(timelimit)))
    if size is not None:
        params.append(("_size", str(size)))
    if next_token is not None:
        params.append(("_next", next_token))
    
    return build_url_with_params(url, params)

def build_search_table_url(
    base_url: str,
    database: str,
    table: str,
    search_term: str,
    search_column: Optional[str] = None,
    columns: Optional[List[str]] = None,
    raw_mode: Optional[bool] = None,
    shape: Optional[str] = None,
    size: Optional[int] = None,
    json_columns: Optional[List[str]] = None,
    next_token: Optional[str] = None
) -> str:
    """Build URL for full-text search within a table."""
    url = safe_url_join(base_url, quote(database), f"{quote(table)}.json")
    params = []
    
    if search_column:
        params.append((f"_search_{search_column}", search_term))
    else:
        params.append(("_search", search_term))
    
    if columns:
        # Add column selection parameters
        for col in columns:
            params.append(("_col", col))
    
    if raw_mode:
        params.append(("_searchmode", "raw"))
    if shape is not None:
        params.append(("_shape", shape))
    if size is not None:
        params.append(("_size", str(size)))
    if json_columns:
        params.append(("_json", ",".join(json_columns)))
    if next_token is not None:
        params.append(("_next", next_token))
    
    return build_url_with_params(url, params)

def build_list_databases_url(base_url: str) -> str:
    """Build URL for listing all databases."""
    return safe_url_join(base_url, ".json")

def build_database_url(base_url: str, database: str, shape: Optional[str] = None, size: Optional[int] = None, next_token: Optional[str] = None) -> str:
    """Build URL for getting database metadata (including tables)."""
    url = safe_url_join(base_url, f"{quote(database)}.json")
    params = []
    if shape is not None:
        params.append(("_shape", shape))
    if size is not None:
        params.append(("_size", str(size)))
    if next_token is not None:
        params.append(("_next", next_token))
    return build_url_with_params(url, params)

def build_describe_table_url(
    base_url: str,
    database: str,
    table: str,
    shape: Optional[str] = None
) -> str:
    """Build URL for getting table schema and metadata."""
    url = safe_url_join(base_url, quote(database), f"{quote(table)}.json")
    params = []
    if shape is not None:
        params.append(("_shape", shape))
    params.append(("_size", "0"))  # Get metadata only, not data rows
    return build_url_with_params(url, params)

# FastMCP Server Setup

mcp = FastMCP(
    name="Datasette Explorer",
    instructions=""  # Will be set dynamically after config is loaded
)

@mcp.tool()
async def execute_sql(
    instance: str, 
    database: str, 
    sql: str,
    shape: Optional[str] = None,
    json_columns: Optional[List[str]] = None,
    trace: bool = False,
    timelimit: Optional[int] = None,
    size: Optional[int] = None,
    next_token: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """Execute SQL query against a Datasette instance.
    
    Args:
        instance: Name of the Datasette instance (from config)
        database: Database name
        sql: SQL query to execute
        shape: JSON shape - "arrays", "objects", or "array" (uses Datasette default if not specified)
        json_columns: List of columns to parse as JSON
        trace: Include query performance trace
        timelimit: Query timeout in milliseconds
        size: Maximum number of results per page (uses Datasette default if not specified)
        next_token: Pagination token from previous response to get next page
        
    Returns:
        Query results and metadata (includes 'next_url' for pagination if more results available)
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_sql_query_url(
            instance_config['url'], database, sql, shape, json_columns, trace, timelimit, size, next_token
        )
        
        if ctx:
            await ctx.info(f"Executing SQL on {instance}/{database}: {sql[:100]}...")
        
        return await make_datasette_request(url, "execute_sql", instance)
        
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in execute_sql: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in execute_sql: {e}")
        raise

@mcp.tool()
async def search_table(
    instance: str,
    database: str, 
    table: str,
    search_term: str,
    search_column: Optional[str] = None,
    columns: Optional[List[str]] = None,
    raw_mode: Optional[bool] = None,
    shape: Optional[str] = None,
    size: Optional[int] = None,
    json_columns: Optional[List[str]] = None,
    next_token: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """Full-text search within a table using Datasette's search functionality.
    
    Args:
        instance: Name of the Datasette instance (from config)
        database: Database name
        table: Table name
        search_term: Text to search for
        search_column: Search only in this column (optional)
        columns: List of columns to return (optional, returns all columns if not specified)
        raw_mode: Enable advanced FTS operators (AND, OR, NOT, NEAR)
        shape: JSON shape - "arrays", "objects", or "array" (uses Datasette default if not specified)
        size: Maximum number of results (uses Datasette default if not specified)
        json_columns: List of columns to parse as JSON
        next_token: Pagination token from previous response to get next page
        
    Returns:
        Search results and metadata (includes 'next_url' for pagination if more results available)
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_search_table_url(
            instance_config['url'], database, table, search_term, search_column, 
            columns, raw_mode, shape, size, json_columns, next_token
        )
        
        if ctx:
            await ctx.info(f"Searching {instance}/{database}/{table} for: {search_term}")
        
        return await make_datasette_request(url, "search_table", instance)
        
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in search_table: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in search_table: {e}")
        raise

@mcp.tool()
async def list_instances(ctx: Context = None) -> Dict[str, Any]:
    """List all configured Datasette instances.
    
    Returns:
        List of instances with their configuration details
    """
    try:
        instances = []
        for name, instance_config in Config['datasette_instances'].items():
            instances.append({
                "name": name,
                "url": instance_config['url'],
                "description": instance_config.get('description', ''),
                "has_auth": bool(instance_config.get('auth_token'))
            })
        
        return {
            "instances": instances,
            "count": len(instances)
        }
        
    except Exception as e:
        if ctx:
            await ctx.error(f"Error listing instances: {e}")
        raise

@mcp.tool()
async def list_databases(instance: str, ctx: Context = None) -> Dict[str, Any]:
    """List all databases in a Datasette instance.
    
    Args:
        instance: Name of the Datasette instance (from config)
        
    Returns:
        List of databases and metadata
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_list_databases_url(instance_config['url'])
        
        result = await make_datasette_request(url, "list_databases", instance)
        
        # Transform the response to match expected database list format
        databases = []
        for key, value in result.items():
            if isinstance(value, dict) and 'path' in value:
                databases.append({
                    "name": key,
                    "path": value.get('path', f'/{key}'),
                    "tables_count": value.get('tables_count', 0),
                    "hidden_tables_count": value.get('hidden_tables_count', 0)
                })
        
        return {
            "databases": databases,
            "instance": instance
        }
            
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in list_databases: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in list_databases: {e}")
        raise

@mcp.tool()
async def list_tables(instance: str, database: str, ctx: Context = None) -> Dict[str, Any]:
    """List all tables in a database.
    
    Args:
        instance: Name of the Datasette instance (from config)
        database: Database name
        
    Returns:
        List of tables and metadata
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_database_url(instance_config['url'], database)
        
        if ctx:
            await ctx.info(f"Listing tables for {instance}/{database}")
        
        return await make_datasette_request(url, "list_tables", instance)
        
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in list_tables: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in list_tables: {e}")
        raise

@mcp.tool()
async def describe_database(instance: str, database: str, ctx: Context = None) -> Dict[str, Any]:
    """Get complete database metadata including all table schemas and column information.
    
    Args:
        instance: Name of the Datasette instance (from config)
        database: Database name
        
    Returns:
        Complete database metadata with all tables and their schemas
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_database_url(instance_config['url'], database)
        
        if ctx:
            await ctx.info(f"Describing database {instance}/{database}")
        
        return await make_datasette_request(url, "describe_database", instance)
        
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in describe_database: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in describe_database: {e}")
        raise

@mcp.tool()
async def describe_table(instance: str, database: str, table: str, ctx: Context = None) -> Dict[str, Any]:
    """Get table schema, column information, and metadata.
    
    Args:
        instance: Name of the Datasette instance (from config)
        database: Database name
        table: Table name
        
    Returns:
        Table schema and metadata
    """
    try:
        instance_config = get_instance_config(Config, instance)
        
        url = build_describe_table_url(instance_config['url'], database, table)
        
        if ctx:
            await ctx.info(f"Describing table {instance}/{database}/{table}")
        
        return await make_datasette_request(url, "describe_table", instance)
        
    except ValueError as e:
        # Configuration errors (instance not found, missing URL, etc.) or Datasette API errors
        if ctx:
            await ctx.error(f"Error in describe_table: {e}")
        raise
    except Exception as e:
        if ctx:
            await ctx.error(f"Unexpected error in describe_table: {e}")
        raise


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(description="Datasette MCP Server")
    
    # Configuration source options
    config_group = parser.add_mutually_exclusive_group()
    config_group.add_argument(
        "--config", 
        type=Path,
        help="Path to configuration file"
    )
    config_group.add_argument(
        "--url",
        help="Datasette instance URL for single instance mode"
    )
    
    # Single instance configuration options
    parser.add_argument(
        "--id",
        help="Instance ID (optional, derived from URL if not specified)"
    )
    parser.add_argument(
        "--description",
        help="Description for the Datasette instance"
    )
    parser.add_argument(
        "--courtesy-delay",
        type=float,
        help="Courtesy delay between requests in seconds (default: 0.5)"
    )
    
    # Transport options
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http", "sse"],
        default="stdio",
        help="Transport protocol (default: stdio)"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to for HTTP transports (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", 
        type=int,
        default=8198,
        help="Port to bind to for HTTP transports (default: 8198)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Global config declaration
    global Config
    
    # Validate CLI arguments for single instance mode
    if args.url:
        # Build config from CLI arguments
        Config = build_config_from_cli(args)
        instance_id = args.id if args.id else derive_id_from_url(args.url)
        logger.info(f"Using single instance mode: {instance_id} -> {args.url}")
    else:
        # Load configuration from file (args.config may be None for auto-discovery)
        Config = load_config(args.config)
        
        if Config is None:
            if args.config:
                logger.error(f"Failed to load configuration file: {args.config}")
            else:
                logger.error("No configuration file found in default locations and no --url specified.")
                logger.error("Either provide --config <file>, use --url for single instance mode, or place a config file in:")
                logger.error("  - ~/.config/datasette-mcp/config.yaml")
                logger.error("  - /etc/datasette-mcp/config.yaml")
            sys.exit(1)
    
    # Validate configuration
    if not validate_config(Config):
        logger.error("Configuration validation failed. Please fix the errors above.")
        sys.exit(1)
    
    logger.info(f"Configured instances: {list(Config['datasette_instances'].keys())}")

    # Set instructions based on configuration
    # Using internal MCP server attribute as workaround
    mcp._mcp_server.instructions = build_instructions(Config)

    # Run the server
    if args.transport == "stdio":
        mcp.run(transport="stdio")
    elif args.transport == "streamable-http":
        mcp.run(
            transport="streamable-http",
            host=args.host,
            port=args.port
        )
    elif args.transport == "sse":
        mcp.run(
            transport="sse",
            host=args.host,
            port=args.port
        )

if __name__ == "__main__":
    main()