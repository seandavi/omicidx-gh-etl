"""
Configuration loading for the data warehouse.

Supports multiple configuration sources with priority:
1. Environment variables (highest priority)
2. Config file (warehouse.yml)
3. Defaults (lowest priority)
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
import yaml
from loguru import logger

from .warehouse import WarehouseConfig


@dataclass
class DeploymentConfig:
    """Configuration for deploying to cloud storage (e.g., Cloudflare R2)."""

    # R2/S3 configuration
    endpoint_url: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    bucket_name: Optional[str] = None
    region: str = 'auto'

    # Deployment paths
    data_prefix: str = 'data'  # Where parquet files go
    catalog_path: str = 'catalog.json'  # Catalog metadata
    database_path: str = 'omicidx.duckdb'  # Remote views database

    # Optional CDN/public URL
    public_url: Optional[str] = None  # e.g., https://data.omicidx.org


# Environment variable names
ENV_PREFIX = "OMICIDX_"
ENV_VARS = {
    'db_path': f'{ENV_PREFIX}DB_PATH',
    'models_dir': f'{ENV_PREFIX}MODELS_DIR',
    'export_dir': f'{ENV_PREFIX}EXPORT_DIR',
    'threads': f'{ENV_PREFIX}THREADS',
    'memory_limit': f'{ENV_PREFIX}MEMORY_LIMIT',
    'temp_directory': f'{ENV_PREFIX}TEMP_DIR',
}

DEPLOYMENT_ENV_VARS = {
    'endpoint_url': f'{ENV_PREFIX}R2_ENDPOINT_URL',
    'access_key_id': f'{ENV_PREFIX}R2_ACCESS_KEY_ID',
    'secret_access_key': f'{ENV_PREFIX}R2_SECRET_ACCESS_KEY',
    'bucket_name': f'{ENV_PREFIX}R2_BUCKET_NAME',
    'region': f'{ENV_PREFIX}R2_REGION',
    'data_prefix': f'{ENV_PREFIX}R2_DATA_PREFIX',
    'catalog_path': f'{ENV_PREFIX}R2_CATALOG_PATH',
    'database_path': f'{ENV_PREFIX}R2_DATABASE_PATH',
    'public_url': f'{ENV_PREFIX}PUBLIC_URL',
}


def load_config(config_path: Optional[str] = None) -> WarehouseConfig:
    """
    Load warehouse configuration from file and environment variables.

    Priority (highest to lowest):
    1. Environment variables (OMICIDX_*)
    2. Config file (warehouse.yml or specified path)
    3. Defaults

    Args:
        config_path: Path to config file (default: warehouse.yml)

    Returns:
        WarehouseConfig instance

    Environment Variables:
        OMICIDX_DB_PATH: Path to warehouse database
        OMICIDX_MODELS_DIR: Path to models directory
        OMICIDX_EXPORT_DIR: Base directory for exports
        OMICIDX_THREADS: Number of DuckDB threads
        OMICIDX_MEMORY_LIMIT: DuckDB memory limit
        OMICIDX_TEMP_DIR: Temporary directory

    Example Config File (warehouse.yml):
        warehouse:
          db_path: omicidx_warehouse.duckdb
          models_dir: omicidx_etl/transformations/models
          export_dir: /data/davsean/omicidx_root/exports
          threads: 16
          memory_limit: 32GB
          temp_directory: /tmp/duckdb
    """
    # Start with defaults
    config_dict = {}

    # Load from config file
    if config_path is None:
        config_path = 'warehouse.yml'

    if Path(config_path).exists():
        logger.debug(f"Loading config from: {config_path}")
        with open(config_path) as f:
            file_config = yaml.safe_load(f)
            if file_config and 'warehouse' in file_config:
                config_dict = file_config['warehouse']
                logger.info(f"Loaded config from {config_path}")
    else:
        logger.debug(f"Config file not found: {config_path}, using defaults")

    # Override with environment variables
    for key, env_var in ENV_VARS.items():
        value = os.getenv(env_var)
        if value is not None:
            # Convert types
            if key == 'threads':
                value = int(value)
            config_dict[key] = value
            logger.debug(f"Config override from env: {key}={value}")

    # Create WarehouseConfig with merged settings
    # Any missing values will use WarehouseConfig defaults
    return WarehouseConfig(**{k: v for k, v in config_dict.items() if v is not None})


def save_config(config: WarehouseConfig, config_path: str = 'warehouse.yml'):
    """
    Save warehouse configuration to file.

    Args:
        config: WarehouseConfig instance
        config_path: Path to save config file
    """
    config_dict = {
        'warehouse': {
            'db_path': config.db_path,
            'models_dir': config.models_dir,
            'export_dir': config.export_dir,
            'threads': config.threads,
            'memory_limit': config.memory_limit,
        }
    }

    if config.temp_directory:
        config_dict['warehouse']['temp_directory'] = config.temp_directory

    with open(config_path, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Saved config to: {config_path}")


def load_deployment_config(config_path: Optional[str] = None) -> Optional[DeploymentConfig]:
    """
    Load deployment configuration from file and environment variables.

    Priority (highest to lowest):
    1. Environment variables (OMICIDX_R2_*)
    2. Config file (warehouse.yml or specified path)
    3. Defaults

    Args:
        config_path: Path to config file (default: warehouse.yml)

    Returns:
        DeploymentConfig instance or None if not configured

    Environment Variables:
        OMICIDX_R2_ENDPOINT_URL: R2/S3 endpoint URL
        OMICIDX_R2_ACCESS_KEY_ID: R2/S3 access key
        OMICIDX_R2_SECRET_ACCESS_KEY: R2/S3 secret key
        OMICIDX_R2_BUCKET_NAME: Bucket name
        OMICIDX_R2_REGION: Region (default: auto)
        OMICIDX_R2_DATA_PREFIX: Prefix for data files
        OMICIDX_R2_CATALOG_PATH: Path to catalog.json
        OMICIDX_R2_DATABASE_PATH: Path to remote database
        OMICIDX_PUBLIC_URL: Public CDN URL
    """
    config_dict = {}

    # Load from config file
    if config_path is None:
        config_path = 'warehouse.yml'

    if Path(config_path).exists():
        with open(config_path) as f:
            file_config = yaml.safe_load(f)
            if file_config and 'deployment' in file_config:
                config_dict = file_config['deployment']
                logger.info(f"Loaded deployment config from {config_path}")

    # Override with environment variables
    for key, env_var in DEPLOYMENT_ENV_VARS.items():
        value = os.getenv(env_var)
        if value is not None:
            config_dict[key] = value
            logger.debug(f"Deployment config override from env: {key}=***")

    # Return None if not configured
    if not config_dict:
        return None

    # Check if minimally configured (need at least bucket_name)
    if 'bucket_name' not in config_dict or not config_dict['bucket_name']:
        logger.warning("Deployment not configured: missing bucket_name")
        return None

    return DeploymentConfig(**config_dict)


def print_config(config: WarehouseConfig, deployment: Optional[DeploymentConfig] = None):
    """Print current configuration."""
    print("\nWarehouse Configuration:")
    print(f"  Database:      {config.db_path}")
    print(f"  Models Dir:    {config.models_dir}")
    print(f"  Export Dir:    {config.export_dir}")
    print(f"  Threads:       {config.threads}")
    print(f"  Memory Limit:  {config.memory_limit}")
    if config.temp_directory:
        print(f"  Temp Dir:      {config.temp_directory}")

    if deployment:
        print("\nDeployment Configuration:")
        print(f"  Bucket:        {deployment.bucket_name}")
        print(f"  Endpoint:      {deployment.endpoint_url or '(default)'}")
        print(f"  Region:        {deployment.region}")
        print(f"  Data Prefix:   {deployment.data_prefix}")
        print(f"  Catalog Path:  {deployment.catalog_path}")
        print(f"  Database Path: {deployment.database_path}")
        if deployment.public_url:
            print(f"  Public URL:    {deployment.public_url}")
    print()
