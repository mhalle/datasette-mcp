#!/usr/bin/env python3
"""
Datasette MCP Server

A Model Context Protocol server that provides read-only access to Datasette instances.
"""
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
from urllib.parse import urlencode, quote
import argparse
import logging

import httpx
import yaml
import json
from fastmcp import FastMCP, Context

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
    instance_config = get_instance_config(instance_name)
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

def find_config_file() -> Optional[Path]:
    """Find datasette-mcp config file in standard locations."""
    
    # 1. Environment variable
    env_config = os.getenv('DATASETTE_MCP_CONFIG')
    if env_config:
        env_path = Path(env_config)
        
        if env_path.is_absolute():
            # Absolute path - use directly
            if env_path.exists():
                return env_path
        else:
            # Relative path - only allow simple filenames (no path separators)
            if env_path == Path(env_path.name):
                # Check in user config directory first, then system config
                user_config_dir = Path.home() / '.config' / 'datasette-mcp'
                user_path = user_config_dir / env_path
                if user_path.exists():
                    return user_path
                    
                system_path = Path('/etc/datasette-mcp') / env_path
                if system_path.exists():
                    return system_path
    
    # 2. User config directory - check both formats
    user_config_dir = Path.home() / '.config' / 'datasette-mcp'
    for filename in ['config.yaml', 'config.yml', 'config.json']:
        user_config = user_config_dir / filename
        if user_config.exists():
            return user_config
    
    # 3. System config directory - check both formats
    for filename in ['config.yaml', 'config.yml', 'config.json']:
        system_config = Path(f'/etc/datasette-mcp/{filename}')
        if system_config.exists():
            return system_config
    
    return None

def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load configuration from file (supports YAML and JSON)."""

    loaded_config = {}
    if config_path is None:
        config_path = find_config_file()
    
    if config_path is None:
        logger.error("No config file found.")
        return None
    
    try:
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() == '.json':
                loaded_config = json.load(f)
            else:
                # Default to YAML for .yaml, .yml, or unknown extensions
                loaded_config = yaml.safe_load(f)
        
        logger.info(f"Loaded config from {config_path}")
        logger.debug(f"Config content: {loaded_config}")
        return loaded_config
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file {config_path}: {e}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file {config_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading config file {config_path}: {e}")
        return None


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration and return True if valid."""
    if not config:
        logger.error("Configuration is empty or None")
        return False
    
    # Check for datasette_instances section
    if not config.get('datasette_instances'):
        logger.error("No 'datasette_instances' section in config")
        return False
    
    instances = config['datasette_instances']
    if not isinstance(instances, dict):
        logger.error("'datasette_instances' must be a dictionary")
        return False
    
    if len(instances) == 0:
        logger.error("No instances configured in 'datasette_instances'")
        return False
    
    # Validate each instance
    for name, instance in instances.items():
        if not isinstance(instance, dict):
            logger.error(f"Instance '{name}' configuration must be a dictionary")
            return False
        
        # Check required fields
        if not instance.get('url'):
            logger.error(f"Instance '{name}' missing required 'url' field")
            return False
        
        url = instance['url']
        if not isinstance(url, str) or not url.strip():
            logger.error(f"Instance '{name}' has invalid 'url' field: must be a non-empty string")
            return False
        
        # Basic URL format validation
        if not (url.startswith('http://') or url.startswith('https://')):
            logger.error(f"Instance '{name}' URL must start with 'http://' or 'https://': {url}")
            return False
        
        # Validate optional fields
        if 'auth_token' in instance and not isinstance(instance['auth_token'], str):
            logger.error(f"Instance '{name}' 'auth_token' must be a string")
            return False
        
        if 'description' in instance and not isinstance(instance['description'], str):
            logger.error(f"Instance '{name}' 'description' must be a string")
            return False
        
        # Warn about unknown fields
        known_fields = {'url', 'auth_token', 'description'}
        unknown_fields = set(instance.keys()) - known_fields
        if unknown_fields:
            logger.warning(f"Instance '{name}' has unknown fields: {', '.join(unknown_fields)}")
    
    # Validate courtesy_delay_seconds if present
    if 'courtesy_delay_seconds' in config:
        courtesy_delay = config['courtesy_delay_seconds']
        if not isinstance(courtesy_delay, (int, float)):
            logger.error("'courtesy_delay_seconds' must be a number")
            return False
        if courtesy_delay < 0:
            logger.error("'courtesy_delay_seconds' must be non-negative")
            return False
    
    logger.info(f"Configuration validated successfully: {len(instances)} instance(s) configured")
    courtesy_delay = config.get('courtesy_delay_seconds', 0.5)
    if courtesy_delay > 0:
        logger.info(f"Courtesy delay enabled: {courtesy_delay}s between requests per instance")
    else:
        logger.info("Courtesy delay disabled")
    return True
    


def get_instance_config(instance: str) -> Dict[str, Any]:
    """Get complete instance configuration including URL and auth headers.
    
    Assumes config has already been validated at startup.
    """
    if instance not in Config['datasette_instances']:
        available = list(Config['datasette_instances'].keys())
        raise ValueError(f"Unknown instance '{instance}'. Available: {available}")
    
    instance_config = Config['datasette_instances'][instance]
    
    # Build complete instance info
    headers = {}
    if 'auth_token' in instance_config:
        headers['Authorization'] = f"Bearer {instance_config['auth_token']}"
    
    return {
        'url': instance_config['url'],
        'headers': headers,
        'description': instance_config.get('description', ''),
        'name': instance
    }

# URL Builder Functions

def build_url_with_params(base_url: str, params: List[tuple]) -> str:
    """Build URL with query parameters."""
    if not params:
        return base_url
    return f"{base_url}?{urlencode(params)}"

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
    url = f"{base_url.rstrip('/')}/{quote(database)}.json"
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
    json_columns: Optional[List[str]] = None
) -> str:
    """Build URL for full-text search within a table."""
    url = f"{base_url.rstrip('/')}/{quote(database)}/{quote(table)}.json"
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
    
    return build_url_with_params(url, params)

def build_list_databases_url(base_url: str) -> str:
    """Build URL for listing all databases."""
    return f"{base_url.rstrip('/')}/.json"

def build_list_tables_url(base_url: str, database: str, shape: Optional[str] = None, size: Optional[int] = None, next_token: Optional[str] = None) -> str:
    """Build URL for listing tables in a database."""
    url = f"{base_url.rstrip('/')}/{quote(database)}.json"
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
    url = f"{base_url.rstrip('/')}/{quote(database)}/{quote(table)}.json"
    params = []
    if shape is not None:
        params.append(("_shape", shape))
    params.append(("_size", "0"))  # Get metadata only, not data rows
    return build_url_with_params(url, params)

# FastMCP Server Setup

mcp = FastMCP(
    name="Datasette Explorer",
    instructions="""
    This server provides read-only access to Datasette instances.
    
    EXPLORATION WORKFLOW:
    1. Use list_instances() to see available Datasette instances
    2. Use list_databases(instance) to see available databases
    3. Use list_tables(instance, database) to see tables in a database  
    4. Use describe_table(instance, database, table) to understand table structure
    5. Use execute_sql() for data queries and analysis
    6. Use search_table() for full-text search when available
    
    SQL DIALECT AND SYNTAX:
    • Datasette uses SQLite3 SQL dialect
    • Column names with spaces or special characters: use [square brackets] like [My Column]
    • SQLite functions available: date(), datetime(), julianday(), etc.
    • Case-insensitive LIKE operator, glob() for pattern matching
    
    SQL BEST PRACTICES:
    • Always use LIMIT for initial exploration: SELECT * FROM table LIMIT 10
    • Use COUNT(*) to understand table sizes: SELECT COUNT(*) FROM table
    • GROUP BY for faceting/aggregation: SELECT category, COUNT(*) FROM products GROUP BY category
    • ORDER BY for sorting: SELECT * FROM users ORDER BY created_date DESC LIMIT 10
    • Combine multiple tables with JOINs when needed
    • Quote column names with spaces: SELECT [First Name], [Last Name] FROM users
    
    FULL-TEXT SEARCH:
    • Use search_table() instead of complex SQLite FTS syntax
    • Try simple terms first: search_table("prod", "blog", "posts", "climate change")
    • Use raw_mode=True for AND/OR/NOT: search_table(..., raw_mode=True) with "term1 AND term2"
    • Search specific columns: search_table(..., search_column="title")
    """
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
        instance_config = get_instance_config(instance)
        
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
        
    Returns:
        Search results and metadata
    """
    try:
        instance_config = get_instance_config(instance)
        
        url = build_search_table_url(
            instance_config['url'], database, table, search_term, search_column, 
            columns, raw_mode, shape, size, json_columns
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
        instance_config = get_instance_config(instance)
        
        url = build_list_databases_url(instance_config['url'])
        
        result = await make_datasette_request(url, "list_databases", instance)
        
        # Transform the response to match expected database list format
        databases = []
        for key, value in result.items():
            if isinstance(value, dict) and 'path' in value:
                databases.append({
                    "name": key,
                    "path": value.get('path', f'/{key}'),
                    "tables_count": len(value.get('tables', [])),
                    "hidden_count": value.get('hidden_count', 0)
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
        instance_config = get_instance_config(instance)
        
        url = build_list_tables_url(instance_config['url'], database)
        
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
        instance_config = get_instance_config(instance)
        
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

def build_config_from_cli(args) -> Dict[str, Any]:
    """Build configuration from CLI arguments for single instance mode."""
    config = {
        'datasette_instances': {
            args.instance: {
                'url': args.url
            }
        }
    }
    
    # Add optional instance fields
    if args.description:
        config['datasette_instances'][args.instance]['description'] = args.description
    if args.auth_token:
        config['datasette_instances'][args.instance]['auth_token'] = args.auth_token
    
    # Add global configuration options
    if args.courtesy_delay is not None:
        config['courtesy_delay_seconds'] = args.courtesy_delay
    
    return config

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
        "--instance",
        help="Instance name for single instance mode (requires --url)"
    )
    
    # Single instance configuration options
    parser.add_argument(
        "--url",
        help="Datasette instance URL (required with --instance)"
    )
    parser.add_argument(
        "--description",
        help="Description for the Datasette instance"
    )
    parser.add_argument(
        "--auth-token",
        help="Bearer token for authentication"
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
    if args.instance:
        if not args.url:
            logger.error("--url is required when using --instance")
            sys.exit(1)
        
        # Build config from CLI arguments
        Config = build_config_from_cli(args)
        logger.info(f"Using single instance mode: {args.instance} -> {args.url}")
    else:
        # Load configuration from file (args.config may be None for auto-discovery)
        Config = load_config(args.config)
        
        if Config is None:
            if args.config:
                logger.error(f"Failed to load configuration file: {args.config}")
            else:
                logger.error("No configuration file found in default locations and no --instance specified.")
                logger.error("Either provide --config <file>, use --instance with --url, or place a config file in:")
                logger.error("  - ~/.config/datasette-mcp/config.yaml")
                logger.error("  - /etc/datasette-mcp/config.yaml")
            sys.exit(1)
    
    # Validate configuration
    if not validate_config(Config):
        logger.error("Configuration validation failed. Please fix the errors above.")
        sys.exit(1)
    
    logger.info(f"Configured instances: {list(Config['datasette_instances'].keys())}")

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