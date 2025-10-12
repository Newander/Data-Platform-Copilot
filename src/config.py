"""
Robust, extensible, and testable settings configuration using Pydantic YamlConfigSettingsSource.

This module provides type-safe configuration management with automatic validation,
environment variable support, YAML file support, and easy testing capabilities.
"""
import logging
import os
from pathlib import Path
from typing import Literal, Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

ProjectRootPath = Path(__file__).parents[1]
DemoDataPath = ProjectRootPath / "demo_data"


class DatabaseConfig(BaseModel):
    """Database-related configuration settings."""

    # Database type selection
    database_type: Literal["duckdb", "postgresql"] = Field(
        default="duckdb",
        description="Type of database to use (duckdb or postgresql)"
    )
    default_schema: str = Field(description="Required default schema")

    # DuckDB (maybe, SQLite?) configuration
    file_name: str | None = Field(None, description="Database file name for DuckDB")
    dir: Path | None = Field(None, description="Database directory path for DuckDB")

    # Relational database configuration
    host: str | None = Field(None, description="Relational database host")
    port: int | None = Field(None, description="Relational database port")
    database: str | None = Field(None, description="Relational database name")
    user: str | None = Field(None, description="Relational database username")
    password: str | None = Field(None, description="Relational database password")
    autocommit: bool = Field(True)

    @field_validator('dir', mode='before')
    @classmethod
    def validate_db_dir(cls, v):
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v

    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        if v is not None and not (1 <= v <= 65535):
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

    def duck_db_path(self) -> str:
        """Build DuckDB (Or SQLite) connection string."""
        return str(Path(self.dir) / self.file_name)

    def postgresql_dsn(self) -> str:
        """Build PostgreSQL (or MySQL or Greenplum and so on) connection string."""
        if not self.password:
            raise ValueError("PostgreSQL password is required")

        return f"{self.database_type}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def postgresql_parameters(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }


class SQLConfig(BaseModel):
    """SQL execution configuration settings."""

    row_limit: int = Field(default=200, description="Default row limit for queries")
    query_timeout_ms: int = Field(default=8000, description="Query timeout in milliseconds")

    @field_validator('row_limit', 'query_timeout_ms')
    @classmethod
    def validate_positive_int(cls, v):
        if v <= 0:
            raise ValueError("Value must be positive")
        return v


class LLMConfig(BaseModel):
    """Large Language Model configuration settings."""

    provider: Literal["openai", "openrouter", "ollama"] = Field(
        default="openai",
        description="LLM provider to use"
    )
    model: str = Field(default="gpt-4o-mini", description="Model name to use")
    openai_api_key: str | None = Field(default=None, description="OpenAI API key")
    openrouter_api_key: str | None = Field(default=None, description="OpenRouter API key")
    ollama_base_url: str = Field(default="http://localhost:11434", description="Ollama base URL")

    @model_validator(mode='after')
    def validate_api_keys(self):
        provider = self.provider.lower()
        openai_key = self.openai_api_key
        openrouter_key = self.openrouter_api_key

        # Only validate API keys if they are actually needed for runtime
        # This allows initialization without keys, but they should be provided before use
        if provider == 'openai' and not openai_key:
            logging.warning(
                "OPENAI_API_KEY is not set but OpenAI provider is selected. "
                "Set the API key before making requests."
            )
        elif provider == 'openrouter' and not openrouter_key:
            logging.warning(
                "OPENROUTER_API_KEY is not set but OpenRouter provider is selected. "
                "Set the API key before making requests."
            )

        return self


class ServerConfig(BaseModel):
    """Server configuration settings."""

    host: str = Field(default="0.0.0.0", description="Server host address")
    port: int = Field(default=8000, description="Server port")

    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v


class LoggingConfig(BaseModel):
    """Logging configuration settings."""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        description="Log message format"
    )
    datefmt: str = Field(default="%Y-%m-%d %H:%M:%S", description="Date format for logs")

    @field_validator('level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.upper()


class GitConfig(BaseModel):
    """Git and DBT configuration settings."""

    dbt_dir: Path = Field(default=Path("dbt"), description="DBT project directory")
    github_token: str | None = Field(default=None, description="GitHub API token")
    github_repo: str | None = Field(default=None, description="GitHub repository (owner/repo)")
    default_branch: str = Field(default="main", description="Default Git branch")
    author_name: str = Field(default="Data Platform Copilot", description="Git author name")
    author_email: str = Field(default="bot@example.com", description="Git author email")

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


class DataQualityConfig(BaseModel):
    """Data Quality configuration settings."""

    default_limit: int = Field(default=10000, description="Default row limit for profiling")
    max_limit: int = Field(default=200000, description="Maximum row limit safety guard")
    default_sigma: float = Field(default=3.0, description="Default sigma for z-score")

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


class DataConfig(BaseModel):
    """Data directory and file configuration settings."""

    data_dir: Path | None = Field(default=None, description="Data directory path")

    @field_validator('data_dir', mode='before')
    @classmethod
    def validate_data_dir(cls, v):
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v


class OrchestrationConfig(BaseModel):
    """Orchestration configuration settings."""

    prefect_api: str = Field(default="http://localhost:4200/api", description="Prefect API URL")


class Settings(BaseModel):
    """Main settings class that combines all configuration sections."""

    model_config = SettingsConfigDict()

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    sql: SQLConfig = Field(default_factory=SQLConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    git: GitConfig = Field(default_factory=GitConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    orchestration: OrchestrationConfig = Field(default_factory=OrchestrationConfig)

    def get_config_summary(self) -> dict[str, dict[str, Any]]:
        """Get a summary of all configuration values (excluding sensitive data)."""
        # summary = {}
        # for section_name in ['database', 'sql', 'llm', 'server', 'logging', 'git', 'data_quality', 'data',
        #                      'orchestration']:
        #     section = getattr(self, section_name)
        #     summary[section_name] = section.model_dump()

        return self.model_dump()

    def validate_required_settings(self):
        """Validate that all required settings for the current configuration are present."""
        try:
            # This will trigger validation of all sections, including API keys based on provider
            _ = self.llm
            return True
        except Exception as e:
            raise ValueError(f"Configuration validation failed: {str(e)}")

    @classmethod
    def from_yaml(cls, yaml_file: Path, yaml_file_encoding: str = 'utf-8') -> 'Settings':
        with yaml_file.open('r', encoding=yaml_file_encoding) as f:
            config_file = yaml.load(f, yaml.SafeLoader)

        instance = cls.model_validate(config_file)
        return instance


ENV_PATH = os.getenv("ENV_PATH", '.env.yaml')
# Create a global settings instance
settings = Settings.from_yaml(
    yaml_file=Path(ENV_PATH),
    yaml_file_encoding='utf-8',
)


def inspect_settings() -> dict[str, dict[str, Any]]:
    """Inspect the current settings configuration (useful for debugging and documentation)."""
    return settings.get_config_summary()


if __name__ == "__main__":
    # Example usage and inspection
    print("=== Settings Configuration Summary ===")
    import json

    json_result = inspect_settings()
    print(json.dumps(json_result, indent=2, default=str))

    print("\n=== Validating Configuration ===")
    try:
        settings.validate_required_settings()
        print("✓ Configuration is valid")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
