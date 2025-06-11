# Datasette MCP

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that provides read-only access to [Datasette](https://datasette.io/) instances. This server enables AI assistants to explore, query, and analyze data from Datasette databases through a standardized interface.

## Features

- **SQL Query Execution**: Run custom SQL queries against Datasette databases
- **Full-Text Search**: Search within tables using Datasette's FTS capabilities
- **Schema Exploration**: List databases, tables, and inspect table schemas
- **Multiple Instances**: Connect to multiple Datasette instances simultaneously
- **Authentication**: Support for Bearer token authentication
- **Request Throttling**: Configurable courtesy delays between requests
- **Multiple Transports**: stdio, HTTP, and Server-Sent Events support

## Installation

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### Install as a tool

```bash
# Install directly from GitHub
uv tool install git+https://github.com/mhalle/datasette-mcp.git

# Check installation
datasette-mcp --help
```

### Run without installation

```bash
# Run directly with uvx (no installation required)
uvx git+https://github.com/mhalle/datasette-mcp.git --url https://your-datasette.com

# Or with config file
uvx git+https://github.com/mhalle/datasette-mcp.git --config /path/to/config.yaml
```

### Development installation

```bash
# Clone and install for development
git clone https://github.com/mhalle/datasette-mcp.git
cd datasette-mcp
uv sync
uv run datasette-mcp --help
```

## Configuration

The server supports two configuration methods:

### 1. Configuration File

Create a YAML or JSON configuration file with your Datasette instances:

```yaml
# ~/.config/datasette-mcp/config.yaml
datasette_instances:
  my_database:
    url: "https://my-datasette.herokuapp.com"
    description: "My production database"
    auth_token: "your-api-token-here"  # optional
  
  local_dev:
    url: "http://localhost:8001"
    description: "Local development database"

# Global settings (optional)
courtesy_delay_seconds: 0.5  # delay between requests
```

The server automatically searches for config files in:
1. `$DATASETTE_MCP_CONFIG` environment variable
2. `~/.config/datasette-mcp/config.{yaml,yml,json}`
3. `/etc/datasette-mcp/config.{yaml,yml,json}`

### 2. Command Line (Single Instance)

For quick single-instance setup:

```bash
datasette-mcp \
  --url https://my-datasette.herokuapp.com \
  --id my_db \
  --description "My database"
```

## Usage

### Basic Startup

```bash
# Use auto-discovered config file
datasette-mcp

# Use specific config file
datasette-mcp --config /path/to/config.yaml

# Single instance mode
datasette-mcp --url https://example.com --id mydb
```

### Transport Options

```bash
# stdio (default, for MCP clients)
datasette-mcp

# HTTP server
datasette-mcp --transport streamable-http --port 8080

# Server-Sent Events
datasette-mcp --transport sse --host 0.0.0.0 --port 8080
```

### Development Usage

When developing or testing:

```bash
# Run from source with uv
uv run datasette-mcp --url https://example.com

# Install in development mode
uv tool install --editable .
```

### All CLI Options

```
--config CONFIG           Path to configuration file
--url URL                 Datasette instance URL for single instance mode
--id ID                   Instance ID (optional, derived from URL if not specified)
--description DESC        Description for the instance
--courtesy-delay FLOAT    Delay between requests in seconds
--transport TRANSPORT     Protocol: stdio, streamable-http, sse
--host HOST               Host for HTTP transports (default: 127.0.0.1)
--port PORT               Port for HTTP transports (default: 8198)
--log-level LEVEL         Logging level: DEBUG, INFO, WARNING, ERROR
```

## Claude Code Integration

To use this MCP server with [Claude Code](https://claude.ai/code):

### 1. Install the server

```bash
uv tool install git+https://github.com/mhalle/datasette-mcp.git
```

### 2. Add to Claude Code

```bash
claude mcp add datasette-mcp -- datasette-mcp --url https://your-datasette-instance.com
```

Or with a configuration file:

```bash
claude mcp add datasette-mcp -- datasette-mcp --config /path/to/config.yaml
```

### 3. Use with scopes (optional)

```bash
claude mcp add -s data-analysis datasette-mcp -- datasette-mcp --url https://analytics.example.com
```

Once added, Claude Code will have access to explore and query your Datasette instances directly within conversations.

## Available Tools

The server provides these MCP tools for AI assistants:

### `list_instances()`
List all configured Datasette instances and their details.

### `list_databases(instance)`
List all databases in a Datasette instance with table counts.

### `describe_database(instance, database)`
Get complete database schema including all table structures, columns, types, and relationships in one efficient call.

### `execute_sql(instance, database, sql, ...)`
Execute custom SQL queries with options for:
- `shape`: Response format ("objects", "arrays", "array")
- `json_columns`: Parse specific columns as JSON
- `trace`: Include performance trace information
- `timelimit`: Query timeout in milliseconds
- `size`: Maximum number of results per page
- `next_token`: Pagination token for getting next page

### `search_table(instance, database, table, search_term, ...)`
Perform full-text search within a table with options for:
- `search_column`: Search only in specific column
- `columns`: Return only specific columns to reduce tokens
- `raw_mode`: Enable advanced FTS operators (AND, OR, NOT)
- `size`: Maximum number of results per page
- `next_token`: Pagination token for getting next page

## Usage Examples

### Exploring Data Structure

```python
# List available instances
instances = await list_instances()

# Explore a specific instance
databases = await list_databases("my_database")

# Get complete database schema with all tables
schema = await describe_database("my_database", "main")
```

### Querying Data

```python
# Get recent users
users = await execute_sql(
    "my_database", 
    "main", 
    "SELECT * FROM users ORDER BY created_date DESC LIMIT 10"
)

# Search for specific content with limited columns
results = await search_table(
    "my_database", 
    "main", 
    "posts", 
    "machine learning",
    columns=["title", "content", "author"],
    size=20
)
```

### Advanced Queries

```python
# Complex aggregation with pagination
stats = await execute_sql(
    "my_database",
    "main",
    """
    SELECT category, COUNT(*) as count, AVG(price) as avg_price
    FROM products 
    WHERE created_date > '2024-01-01'
    GROUP BY category
    ORDER BY count DESC
    """,
    size=50
)

# Search with advanced operators
results = await search_table(
    "my_database",
    "main",
    "articles",
    "python AND (fastapi OR django)",
    raw_mode=True
)
```

## Security Considerations

- The server provides **read-only** access to Datasette instances
- Authentication tokens are passed as Bearer tokens to Datasette
- No write operations are supported
- SQL queries are subject to Datasette's built-in security restrictions
- Request throttling helps prevent overwhelming target servers

## Error Handling

The server provides detailed error messages for:
- Invalid SQL queries
- Missing or inaccessible databases/tables
- Authentication failures
- Network timeouts
- Configuration errors

## Logging

Configure logging levels for debugging:

```bash
datasette-mcp --log-level DEBUG
```

Log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`

### Tool Management

```bash
# List installed tools
uv tool list

# Upgrade to latest version
uv tool upgrade datasette-mcp

# Uninstall
uv tool uninstall datasette-mcp
```

## Contributing

This server is built with [FastMCP](https://gofastmcp.com/), making it easy to extend with additional tools and functionality. The codebase follows MCP best practices for server development.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Related Projects

- [Datasette](https://datasette.io/) - Data exploration tool
- [FastMCP](https://gofastmcp.com/) - Python MCP framework
- [Model Context Protocol](https://modelcontextprotocol.io/) - Standard for AI tool integration