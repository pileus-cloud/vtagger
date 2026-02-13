"""
VTagger Configuration Module.

Handles loading configuration from environment variables and config files.
"""

import os
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load .env file from project root (if present)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path)


class Settings(BaseSettings):
    """Application settings loaded from environment or config file."""

    # Database
    database_path: str = "./data/vtagger.db"

    # API Server
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    dev_mode: bool = True

    # CORS
    cors_origins: list = ["http://localhost:3000", "http://127.0.0.1:3000"]

    # Umbrella API
    umbrella_api_base: str = "https://api.umbrellacost.io/api"

    # File paths
    output_dir: str = "./data/output"

    # Sync settings
    batch_size: int = 1000
    sync_schedule: str = "0 2 * * *"

    # Retention
    retention_days: int = 90

    # Master key for encryption (optional - derived from machine if not set)
    vtagger_master_key: Optional[str] = None

    class Config:
        env_prefix = "VTAGGER_"
        case_sensitive = False


def get_config_path() -> Path:
    """Get the config file path."""
    return Path.home() / ".vtagger" / "config.yaml"


def load_config_file() -> dict:
    """Load configuration from YAML file if exists."""
    config_path = get_config_path()
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_settings() -> Settings:
    """Get application settings."""
    file_config = load_config_file()

    env_overrides = {}

    if "database" in file_config:
        if "path" in file_config["database"]:
            env_overrides["VTAGGER_DATABASE_PATH"] = file_config["database"]["path"]

    if "api" in file_config:
        if "base_url" in file_config["api"]:
            env_overrides["VTAGGER_UMBRELLA_API_BASE"] = file_config["api"]["base_url"]

    if "sync" in file_config:
        if "batch_size" in file_config["sync"]:
            env_overrides["VTAGGER_BATCH_SIZE"] = str(file_config["sync"]["batch_size"])
        if "schedule" in file_config["sync"]:
            env_overrides["VTAGGER_SYNC_SCHEDULE"] = file_config["sync"]["schedule"]

    if "paths" in file_config:
        if "output" in file_config["paths"]:
            env_overrides["VTAGGER_OUTPUT_DIR"] = file_config["paths"]["output"]

    if "logging" in file_config:
        if "level" in file_config["logging"]:
            env_overrides["VTAGGER_LOG_LEVEL"] = file_config["logging"]["level"]

    for key, value in env_overrides.items():
        if key not in os.environ:
            os.environ[key] = value

    return Settings()


# Global settings instance
settings = get_settings()
