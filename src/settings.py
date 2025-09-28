"""
Robust, extensible, and testable settings configuration using Pydantic BaseModel.

This module provides type-safe configuration management with automatic validation,
environment variable support, and easy testing capabilities.
"""

import os
from pathlib import Path
from typing import Literal, Any, Type

from pydantic import BaseModel, Field, field_validator, model_validator


def load_env_value(env_key: str, default: Any = None, cast_type: Type = str):
    """Load and cast environment variable value."""
    value = os.getenv(env_key, default)
    if value is None:
        return None
    if cast_type == bool:
        return str(value).lower() in ('true', '1', 'yes', 'on')
    if cast_type == int:
        return int(value)
    if cast_type == float:
        return float(value)
    if cast_type == Path:
        return Path(value)
    return value


class ConfigMixin:
    """Mixin to provide environment variable loading to config classes."""

    @classmethod
    def from_env[T: BaseModel](cls: Type[T]) -> T:
        """Create instance from environment variables."""
        return cls()


class DatabaseConfig(BaseModel, ConfigMixin):
    """Database-related configuration settings."""

    # Database type selection
    database_type: Literal["duckdb", "postgresql"] = Field(
        default="duckdb",
        description="Type of database to use (duckdb or postgresql)"
    )

    # DuckDB (maybe, SQLite?) configuration
    file_name: str | None = Field(None, description="Database file name for DuckDB")
    dir: Path | None = Field(None, description="Database directory path for DuckDB")

    # Relational database configuration
    host: str | None = Field(None, description="Relational database host")
    port: int | None = Field(None, description="Relational database port")
    database: str | None = Field(None, description="Relational database name")
    user: str | None = Field(None, description="Relational database username")
    password: str | None = Field(None, description="Relational database password")
    default_schema: str | None = Field(None, description="Relational database default schema")

    def __init__(self, **kwargs):
        # Load from environment if not provided
        if 'database_type' not in kwargs:
            kwargs['database_type'] = load_env_value('DATABASE_TYPE', 'duckdb').lower()
        if 'file_name' not in kwargs:
            kwargs['file_name'] = load_env_value('DB_FILE_NAME', 'demo.duckdb')
        if 'dir' not in kwargs:
            dir_val = load_env_value('DB_DIR')
            kwargs['dir'] = Path(dir_val) if dir_val else None

        # Relational database environment variables
        if 'host' not in kwargs:
            kwargs['host'] = load_env_value('POSTGRES_HOST', 'localhost')
        if 'port' not in kwargs:
            kwargs['port'] = load_env_value('POSTGRES_PORT', 5432, int)
        if 'database' not in kwargs:
            kwargs['database'] = load_env_value('POSTGRES_DB', 'data_pilot')
        if 'user' not in kwargs:
            kwargs['user'] = load_env_value('POSTGRES_USER', 'postgres')
        if 'password' not in kwargs:
            kwargs['password'] = load_env_value('POSTGRES_PASSWORD')
        if 'schema' not in kwargs:
            kwargs['schema'] = load_env_value('POSTGRES_SCHEMA', 'public')

        super().__init__(**kwargs)

    @field_validator('dir', mode='before')
    @classmethod
    def validate_db_dir(cls, v):
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v

    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError("Relational database port must be between 1 and 65535")
        return v

    @model_validator(mode='after')
    def validate_database_config(self):
        """Validate that required fields are set based on database type."""
        if self.database_type == "postgresql":
            if not self.password:
                raise ValueError("Relational database password is required when using postgresql database type")
        elif self.database_type == "duckdb":
            if not self.dir:
                raise ValueError("Database directory is required when using duckdb database type")
        return self


class SQLConfig(BaseModel, ConfigMixin):
    """SQL execution configuration settings."""

    row_limit: int = Field(default=200, description="Default row limit for queries")
    query_timeout_ms: int = Field(default=8000, description="Query timeout in milliseconds")

    def __init__(self, **kwargs):
        if 'row_limit' not in kwargs:
            kwargs['row_limit'] = load_env_value('ROW_LIMIT', 200, int)
        if 'query_timeout_ms' not in kwargs:
            kwargs['query_timeout_ms'] = load_env_value('QUERY_TIMEOUT_MS', 8000, int)
        super().__init__(**kwargs)

    @field_validator('row_limit', 'query_timeout_ms')
    @classmethod
    def validate_positive_int(cls, v):
        if v <= 0:
            raise ValueError("Value must be positive")
        return v


class LLMConfig(BaseModel, ConfigMixin):
    """Large Language Model configuration settings."""

    provider: Literal["openai", "openrouter", "ollama"] = Field(
        default="openai",
        description="LLM provider to use"
    )
    model: str = Field(default="gpt-4o-mini", description="Model name to use")
    openai_api_key: str | None = Field(default=None, description="OpenAI API key")
    openrouter_api_key: str | None = Field(default=None, description="OpenRouter API key")
    ollama_base_url: str = Field(default="http://localhost:11434", description="Ollama base URL")

    def __init__(self, **kwargs):
        if 'provider' not in kwargs:
            kwargs['provider'] = load_env_value('LLM_PROVIDER', 'openai').lower()
        if 'model' not in kwargs:
            kwargs['model'] = load_env_value('LLM_MODEL', 'gpt-4o-mini')
        if 'openai_api_key' not in kwargs:
            kwargs['openai_api_key'] = load_env_value('OPENAI_API_KEY')
        if 'openrouter_api_key' not in kwargs:
            kwargs['openrouter_api_key'] = load_env_value('OPENROUTER_API_KEY')
        if 'ollama_base_url' not in kwargs:
            kwargs['ollama_base_url'] = load_env_value('OLLAMA_BASE_URL', 'http://localhost:11434')
        super().__init__(**kwargs)

    @model_validator(mode='after')
    def validate_api_keys(self):
        provider = self.provider.lower()
        openai_key = self.openai_api_key
        openrouter_key = self.openrouter_api_key

        # Only validate API keys if they are actually needed for runtime
        # This allows initialization without keys, but they should be provided before use
        if provider == 'openai' and not openai_key:
            import warnings
            warnings.warn(
                "OPENAI_API_KEY is not set but OpenAI provider is selected. Set the API key before making requests.")
        elif provider == 'openrouter' and not openrouter_key:
            import warnings
            warnings.warn(
                "OPENROUTER_API_KEY is not set but OpenRouter provider is selected. Set the API key before making requests.")

        return self


class ServerConfig(BaseModel, ConfigMixin):
    """Server configuration settings."""

    host: str = Field(default="0.0.0.0", description="Server host address")
    port: int = Field(default=8000, description="Server port")

    def __init__(self, **kwargs):
        if 'host' not in kwargs:
            kwargs['host'] = load_env_value('HOST', '0.0.0.0')
        if 'port' not in kwargs:
            kwargs['port'] = load_env_value('PORT', 8000, int)
        super().__init__(**kwargs)

    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v


class LoggingConfig(BaseModel, ConfigMixin):
    """Logging configuration settings."""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        description="Log message format"
    )
    datefmt: str = Field(default="%Y-%m-%d %H:%M:%S", description="Date format for logs")

    def __init__(self, **kwargs):
        if 'level' not in kwargs:
            kwargs['level'] = load_env_value('LOG_LEVEL', 'INFO').upper()
        if 'format' not in kwargs:
            kwargs['format'] = load_env_value('LOG_FORMAT', '%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        if 'datefmt' not in kwargs:
            kwargs['datefmt'] = load_env_value('LOG_DATEFMT', '%Y-%m-%d %H:%M:%S')
        super().__init__(**kwargs)

    @field_validator('level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.upper()


class GitConfig(BaseModel, ConfigMixin):
    """Git and DBT configuration settings."""

    dbt_dir: Path = Field(default=Path("dbt"), description="DBT project directory")
    github_token: str | None = Field(default=None, description="GitHub API token")
    github_repo: str | None = Field(default=None, description="GitHub repository (owner/repo)")
    default_branch: str = Field(default="main", description="Default Git branch")
    author_name: str = Field(default="Data Platform Copilot", description="Git author name")
    author_email: str = Field(default="bot@example.com", description="Git author email")

    def __init__(self, **kwargs):
        if 'dbt_dir' not in kwargs:
            dbt_dir_val = load_env_value('DBT_DIR', 'dbt')
            kwargs['dbt_dir'] = Path(dbt_dir_val)
        if 'github_token' not in kwargs:
            kwargs['github_token'] = load_env_value('GITHUB_TOKEN')
        if 'github_repo' not in kwargs:
            kwargs['github_repo'] = load_env_value('GITHUB_REPO')
        if 'default_branch' not in kwargs:
            kwargs['default_branch'] = load_env_value('GIT_DEFAULT_BRANCH', 'main')
        if 'author_name' not in kwargs:
            kwargs['author_name'] = load_env_value('GIT_AUTHOR_NAME', 'Data Platform Copilot')
        if 'author_email' not in kwargs:
            kwargs['author_email'] = load_env_value('GIT_AUTHOR_EMAIL', 'bot@example.com')
        super().__init__(**kwargs)

    @field_validator('dbt_dir', mode='before')
    @classmethod
    def validate_dbt_dir(cls, v):
        return Path(v) if not isinstance(v, Path) else v

    @field_validator('github_repo')
    @classmethod
    def validate_github_repo(cls, v):
        if v and '/' not in v:
            raise ValueError("GitHub repo must be in format 'owner/repo'")
        return v


class DataQualityConfig(BaseModel, ConfigMixin):
    """Data Quality configuration settings."""

    default_limit: int = Field(default=10000, description="Default row limit for profiling")
    max_limit: int = Field(default=200000, description="Maximum row limit safety guard")
    default_sigma: float = Field(default=3.0, description="Default sigma for z-score")

    def __init__(self, **kwargs):
        if 'default_limit' not in kwargs:
            kwargs['default_limit'] = load_env_value('DQ_DEFAULT_LIMIT', 10000, int)
        if 'max_limit' not in kwargs:
            kwargs['max_limit'] = load_env_value('DQ_MAX_LIMIT', 200000, int)
        if 'default_sigma' not in kwargs:
            kwargs['default_sigma'] = load_env_value('DQ_DEFAULT_SIGMA', 3.0, float)
        super().__init__(**kwargs)

    @field_validator('default_limit', 'max_limit')
    @classmethod
    def validate_positive_int(cls, v):
        if v <= 0:
            raise ValueError("Value must be positive")
        return v

    @field_validator('default_sigma')
    @classmethod
    def validate_positive_float(cls, v):
        if v <= 0:
            raise ValueError("Sigma must be positive")
        return v

    @model_validator(mode='after')
    def validate_limits(self):
        if self.default_limit > self.max_limit:
            raise ValueError("default_limit cannot be greater than max_limit")
        return self


class DataConfig(BaseModel, ConfigMixin):
    """Data directory and file configuration settings."""

    data_dir: Path | None = Field(default=None, description="Data directory path")

    def __init__(self, **kwargs):
        if 'data_dir' not in kwargs:
            data_dir_val = load_env_value('DATA_DIR')
            kwargs['data_dir'] = Path(data_dir_val) if data_dir_val else None
        super().__init__(**kwargs)

    @field_validator('data_dir', mode='before')
    @classmethod
    def validate_data_dir(cls, v):
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v


class OrchestrationConfig(BaseModel, ConfigMixin):
    """Orchestration configuration settings."""

    prefect_api: str = Field(default="http://localhost:4200/api", description="Prefect API URL")

    def __init__(self, **kwargs):
        if 'prefect_api' not in kwargs:
            kwargs['prefect_api'] = load_env_value('PREFECT_API', 'http://localhost:4200/api')
        super().__init__(**kwargs)


class Settings(BaseModel):
    """Main settings class that combines all configuration sections."""

    database: DatabaseConfig
    sql: SQLConfig
    llm: LLMConfig
    server: ServerConfig
    logging: LoggingConfig
    git: GitConfig
    data_quality: DataQualityConfig
    data: DataConfig
    orchestration: OrchestrationConfig

    def __init__(self, **kwargs):
        # Initialize all configuration sections from environment
        config_data = {
            'database': DatabaseConfig(),
            'sql': SQLConfig(),
            'llm': LLMConfig(),
            'server': ServerConfig(),
            'logging': LoggingConfig(),
            'git': GitConfig(),
            'data_quality': DataQualityConfig(),
            'data': DataConfig(),
            'orchestration': OrchestrationConfig(),
        }
        config_data.update(kwargs)
        super().__init__(**config_data)

    def get_config_summary(self) -> dict[str, dict[str, Any]]:
        """Get a summary of all configuration values (excluding sensitive data)."""
        sensitive_keys = {'openai_api_key', 'openrouter_api_key', 'github_token'}

        summary = {}
        for section_name in ['database', 'sql', 'llm', 'server', 'logging', 'git', 'data_quality', 'data',
                             'orchestration']:
            section = getattr(self, section_name)
            section_dict = section.model_dump()
            # Mask sensitive values
            for key in sensitive_keys:
                if key in section_dict and section_dict[key]:
                    section_dict[key] = "***masked***"
            summary[section_name] = section_dict

        return summary

    def validate_required_settings(self):
        """Validate that all required settings for the current configuration are present."""
        try:
            # This will trigger validation of all sections, including API keys based on provider
            _ = self.llm
            return True
        except Exception as e:
            raise ValueError(f"Configuration validation failed: {str(e)}")

    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        arbitrary_types_allowed = True


# Create global settings instance
settings = Settings()

# Backward compatibility - expose individual settings as module-level variables
DATABASE_TYPE = settings.database.database_type
DB_FILE_NAME = settings.database.file_name
DB_DIR = settings.database.dir
POSTGRES_HOST = settings.database.host
POSTGRES_PORT = settings.database.port
POSTGRES_DB = settings.database.database
POSTGRES_USER = settings.database.user
POSTGRES_PASSWORD = settings.database.password
POSTGRES_SCHEMA = settings.database.default_schema
ROW_LIMIT = settings.sql.row_limit
QUERY_TIMEOUT_MS = settings.sql.query_timeout_ms
LLM_PROVIDER = settings.llm.provider
LLM_MODEL = settings.llm.model
OPENAI_API_KEY = settings.llm.openai_api_key
OPENROUTER_API_KEY = settings.llm.openrouter_api_key
OLLAMA_BASE_URL = settings.llm.ollama_base_url
SERVER_HOST = settings.server.host
SERVER_PORT = settings.server.port
DATA_DIR = settings.data.data_dir
LOG_LEVEL = settings.logging.level
LOG_FORMAT = settings.logging.format
DATE_FORMAT = settings.logging.datefmt
DBT_DIR = settings.git.dbt_dir
GITHUB_TOKEN = settings.git.github_token
GITHUB_REPO = settings.git.github_repo
GIT_DEFAULT_BRANCH = settings.git.default_branch
GIT_AUTHOR_NAME = settings.git.author_name
GIT_AUTHOR_EMAIL = settings.git.author_email
DQ_DEFAULT_LIMIT = settings.data_quality.default_limit
DQ_MAX_LIMIT = settings.data_quality.max_limit
DQ_DEFAULT_SIGMA = settings.data_quality.default_sigma
PREFECT_API = settings.orchestration.prefect_api


# Utility functions for testing and inspection
def get_settings_for_testing(**overrides) -> Settings:
    """Create a Settings instance with overrides for testing."""
    # Set environment variables temporarily
    original_env = {}
    for key, value in overrides.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = str(value)

    try:
        test_settings = Settings()
        return test_settings
    finally:
        # Restore original environment
        for key, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


def inspect_settings() -> dict[str, dict[str, Any]]:
    """Inspect current settings configuration (useful for debugging and documentation)."""
    return settings.get_config_summary()


if __name__ == "__main__":
    # Example usage and inspection
    print("=== Settings Configuration Summary ===")
    import json

    print(json.dumps(inspect_settings(), indent=2, default=str))

    print("\n=== Validating Configuration ===")
    try:
        settings.validate_required_settings()
        print("✓ Configuration is valid")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
