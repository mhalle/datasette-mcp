"""
Configuration handling for Datasette MCP Server.

Handles loading, validation, and building of configuration from files and CLI arguments.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)


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


def load_config(config_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Load configuration from file (supports YAML and JSON)."""

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


def derive_id_from_url(url: str) -> str:
    """Derive instance ID from URL by escaping special characters and prefixing with _"""
    import re
    
    # Replace all non-alphanumeric characters with underscores
    escaped = re.sub(r'[^a-zA-Z0-9]', '_', url)
    
    # Remove multiple consecutive underscores
    escaped = re.sub(r'_+', '_', escaped)
    
    # Remove leading/trailing underscores, then add single leading underscore
    escaped = escaped.strip('_')
    
    return f"_{escaped}"


def build_config_from_cli(args) -> Dict[str, Any]:
    """Build configuration from CLI arguments for single instance mode."""
    # Use provided ID or derive from URL
    instance_id = args.id if args.id else derive_id_from_url(args.url)
    
    config = {
        'datasette_instances': {
            instance_id: {
                'url': args.url
            }
        }
    }
    
    # Add optional instance fields
    if args.description:
        config['datasette_instances'][instance_id]['description'] = args.description
        # For single instance mode, also use description as global description
        config['description'] = args.description
    
    # Add global configuration options
    if args.courtesy_delay is not None:
        config['courtesy_delay_seconds'] = args.courtesy_delay
    
    return config


def get_instance_config(config: Dict[str, Any], instance: str) -> Dict[str, Any]:
    """Get complete instance configuration including URL and auth headers.
    
    Assumes config has already been validated at startup.
    """
    if instance not in config['datasette_instances']:
        available = list(config['datasette_instances'].keys())
        raise ValueError(f"Unknown instance '{instance}'. Available: {available}")
    
    instance_config = config['datasette_instances'][instance]
    
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


def build_instructions(config: Dict[str, Any]) -> str:
    """Build enhanced instructions with dataset description."""
    
    # Base instructions
    base_instructions = """
    This server provides read-only access to Datasette instances.
    
    EXPLORATION WORKFLOW:
    1. Use list_instances() to see available Datasette instances
    2. Use list_databases(instance) to see available databases
    3. Use describe_database(instance, database) to get complete database schema with all tables and columns - MOST EFFICIENT
    4. Use execute_sql() for data queries and analysis
    5. Use search_table() for full-text search when available
    
    ALTERNATIVE WORKFLOW (less efficient):
    • Use list_tables(instance, database) for just table names and counts
    • Use describe_table(instance, database, table) for individual table schemas
    
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
    
    # Build dataset description section
    dataset_section = ""
    
    # Check for global description
    global_description = config.get('description')
    if global_description:
        dataset_section = f"DATASET DESCRIPTION:\n{global_description}\n\n"
    
    # If no global description, check for instance descriptions
    elif config.get('datasette_instances'):
        instance_descriptions = []
        for name, instance_config in config['datasette_instances'].items():
            description = instance_config.get('description', '')
            if description:
                instance_descriptions.append(f"- {name}: {description}")
        
        if instance_descriptions:
            dataset_section = "DATASET DESCRIPTION:\nAvailable instances:\n" + "\n".join(instance_descriptions) + "\n\n"
    
    return dataset_section + base_instructions.strip()