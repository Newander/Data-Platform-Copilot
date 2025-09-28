# Settings System Documentation

## Overview

The Data Pilot project now uses a robust, extensible, and testable settings configuration system built with Pydantic BaseModel. This system provides type safety, automatic validation, environment variable support, and comprehensive inspection capabilities.

## Key Features

### 🔒 **Type Safety & Validation**
- All configuration values are type-checked at runtime
- Automatic validation of data types, ranges, and formats
- Clear error messages for invalid configurations

### 🌍 **Environment Variable Support**
- Automatic loading from environment variables
- Sensible defaults for all settings
- Support for multiple data types (str, int, float, bool, Path)

### 📊 **Configuration Inspection**
- Built-in configuration summary with sensitive data masking
- Easy debugging and documentation generation
- Runtime configuration validation

### 🧪 **Testing Support**
- Utility functions for testing with configuration overrides
- Environment isolation for tests
- Comprehensive test coverage

### 🔄 **Backward Compatibility**
- All existing imports continue to work
- Module-level variables preserved
- No breaking changes for existing code

## Configuration Sections

The settings are organized into logical sections:

### Database Configuration (`DatabaseConfig`)
```python
from src.settings import settings

# Access database settings
db_file = settings.database.file_name  # "demo.duckdb"
db_dir = settings.database.dir         # Optional[Path]
```

**Environment Variables:**
- `DB_FILE_NAME`: Database file name (default: "demo.duckdb")
- `DB_DIR`: Database directory path (optional)

### SQL Configuration (`SQLConfig`)
```python
# Access SQL settings
row_limit = settings.sql.row_limit              # 200
timeout = settings.sql.query_timeout_ms         # 8000
```

**Environment Variables:**
- `ROW_LIMIT`: Default row limit for queries (default: 200)
- `QUERY_TIMEOUT_MS`: Query timeout in milliseconds (default: 8000)

**Validation:**
- Both values must be positive integers

### LLM Configuration (`LLMConfig`)
```python
# Access LLM settings
provider = settings.llm.provider                # "openai" | "openrouter" | "ollama"
model = settings.llm.model                      # "gpt-4o-mini"
api_key = settings.llm.openai_api_key          # Optional[str]
```

**Environment Variables:**
- `LLM_PROVIDER`: LLM provider to use (default: "openai")
- `LLM_MODEL`: Model name to use (default: "gpt-4o-mini")
- `OPENAI_API_KEY`: OpenAI API key (required for openai provider)
- `OPENROUTER_API_KEY`: OpenRouter API key (required for openrouter provider)
- `OLLAMA_BASE_URL`: Ollama base URL (default: "http://localhost:11434")

**Validation:**
- Warns if API key is missing for selected provider
- Provider must be one of: "openai", "openrouter", "ollama"

### Server Configuration (`ServerConfig`)
```python
# Access server settings
host = settings.server.host                    # "0.0.0.0"
port = settings.server.port                    # 8000
```

**Environment Variables:**
- `HOST`: Server host address (default: "0.0.0.0")
- `PORT`: Server port (default: 8000)

**Validation:**
- Port must be between 1 and 65535

### Logging Configuration (`LoggingConfig`)
```python
# Access logging settings
level = settings.logging.level                 # "INFO"
format = settings.logging.format               # "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
datefmt = settings.logging.datefmt             # "%Y-%m-%d %H:%M:%S"
```

**Environment Variables:**
- `LOG_LEVEL`: Logging level (default: "INFO")
- `LOG_FORMAT`: Log message format
- `LOG_DATEFMT`: Date format for logs

**Validation:**
- Level must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL (case-insensitive)

### Git Configuration (`GitConfig`)
```python
# Access Git settings
dbt_dir = settings.git.dbt_dir                 # Path("dbt")
token = settings.git.github_token              # Optional[str]
repo = settings.git.github_repo                # Optional[str]
```

**Environment Variables:**
- `DBT_DIR`: DBT project directory (default: "dbt")
- `GITHUB_TOKEN`: GitHub API token (optional)
- `GITHUB_REPO`: GitHub repository in "owner/repo" format (optional)
- `GIT_DEFAULT_BRANCH`: Default Git branch (default: "main")
- `GIT_AUTHOR_NAME`: Git author name (default: "Data Platform Copilot")
- `GIT_AUTHOR_EMAIL`: Git author email (default: "bot@example.com")

**Validation:**
- GitHub repo must be in "owner/repo" format if provided

### Data Quality Configuration (`DataQualityConfig`)
```python
# Access data quality settings
default_limit = settings.data_quality.default_limit    # 10000
max_limit = settings.data_quality.max_limit            # 200000
sigma = settings.data_quality.default_sigma            # 3.0
```

**Environment Variables:**
- `DQ_DEFAULT_LIMIT`: Default row limit for profiling (default: 10000)
- `DQ_MAX_LIMIT`: Maximum row limit safety guard (default: 200000)
- `DQ_DEFAULT_SIGMA`: Default sigma for z-score (default: 3.0)

**Validation:**
- All values must be positive
- default_limit cannot be greater than max_limit

## Usage Patterns

### Basic Usage
```python
from src.settings import settings

# Access configuration values
database_file = settings.database.file_name
row_limit = settings.sql.row_limit
llm_provider = settings.llm.provider
```

### Backward Compatibility (Legacy Style)
```python
# These imports still work for existing code
from src.settings import DB_FILE_NAME, ROW_LIMIT, LLM_PROVIDER

print(f"Database: {DB_FILE_NAME}")
print(f"Row limit: {ROW_LIMIT}")
print(f"LLM Provider: {LLM_PROVIDER}")
```

### Configuration Inspection
```python
from src.settings import inspect_settings
import json

# Get a summary of all settings (with sensitive data masked)
config_summary = inspect_settings()
print(json.dumps(config_summary, indent=2, default=str))
```

### Testing with Configuration Overrides
```python
from src.settings import get_settings_for_testing

# Create test settings with overrides
test_settings = get_settings_for_testing(
    OPENAI_API_KEY='test-key',
    ROW_LIMIT='100',
    HOST='127.0.0.1'
)

# Use test_settings in your tests
assert test_settings.llm.openai_api_key == 'test-key'
assert test_settings.sql.row_limit == 100
assert test_settings.server.host == '127.0.0.1'

# Environment is automatically restored after the function
```

### Runtime Validation
```python
from src.settings import settings

# Validate current configuration
try:
    settings.validate_required_settings()
    print("✓ Configuration is valid")
except ValueError as e:
    print(f"✗ Configuration error: {e}")
```

## Environment Variable Examples

Create a `.env` file or set environment variables:

```bash
# Database settings
DB_FILE_NAME=production.duckdb
DB_DIR=/data/databases

# SQL settings
ROW_LIMIT=500
QUERY_TIMEOUT_MS=15000

# LLM settings
LLM_PROVIDER=openai
LLM_MODEL=gpt-4
OPENAI_API_KEY=your-openai-api-key-here

# Server settings
HOST=127.0.0.1
PORT=8080

# Logging settings
LOG_LEVEL=DEBUG
LOG_FORMAT=%(asctime)s [%(levelname)s] %(name)s: %(message)s

# Git settings
DBT_DIR=/custom/dbt
GITHUB_TOKEN=your-github-token
GITHUB_REPO=myorg/myrepo

# Data Quality settings
DQ_DEFAULT_LIMIT=5000
DQ_MAX_LIMIT=100000
DQ_DEFAULT_SIGMA=2.5
```

## Error Handling

The settings system provides clear error messages for common issues:

### Missing Required API Keys
```python
# When using OpenAI provider without API key
UserWarning: OPENAI_API_KEY is not set but OpenAI provider is selected. 
Set the API key before making requests.
```

### Invalid Values
```python
# Invalid port number
ValidationError: Port must be between 1 and 65535

# Invalid log level
ValidationError: Invalid log level. Must be one of: {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}

# Negative values where positive required
ValidationError: Value must be positive
```

### Invalid GitHub Repository Format
```python
# Invalid repo format
ValidationError: GitHub repo must be in format 'owner/repo'
```

## Migration Guide

### For New Code
Use the structured approach:
```python
from src.settings import settings

# Recommended: Access through sections
database_file = settings.database.file_name
row_limit = settings.sql.row_limit
```

### For Existing Code
No changes required - all existing imports continue to work:
```python
from src.settings import DB_FILE_NAME, ROW_LIMIT, LLM_PROVIDER
# This continues to work exactly as before
```

## Best Practices

### 1. Use Structured Access for New Code
```python
# Good: Clear and organized
settings.database.file_name
settings.llm.provider

# Avoid: Direct module imports for new code
from src.settings import DB_FILE_NAME, LLM_PROVIDER
```

### 2. Validate Configuration at Startup
```python
def main():
    try:
        settings.validate_required_settings()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Continue with application startup
```

### 3. Use Environment Variables for Deployment
```python
# Production deployment
export LLM_PROVIDER=openai
export OPENAI_API_KEY=your-production-key
export LOG_LEVEL=INFO
export DB_DIR=/production/data

# Development
export LLM_PROVIDER=ollama
export LOG_LEVEL=DEBUG
```

### 4. Testing with Configuration Overrides
```python
def test_with_custom_config():
    test_settings = get_settings_for_testing(
        LLM_PROVIDER='ollama',
        ROW_LIMIT='50'
    )
    
    # Run tests with custom configuration
    result = some_function_using_settings(test_settings)
    assert result is not None
```

## Advanced Features

### Configuration Summary
```python
from src.settings import settings

# Get configuration summary (sensitive data masked)
summary = settings.get_config_summary()

# Sections available:
# - database, sql, llm, server, logging
# - git, data_quality, data, orchestration
```

### Custom Configuration Classes
The system is extensible. You can create additional configuration sections by following the existing patterns:

```python
class MyCustomConfig(BaseModel, ConfigMixin):
    """Custom configuration section."""
    
    my_setting: str = Field(default="default_value", description="My custom setting")
    
    def __init__(self, **kwargs):
        if 'my_setting' not in kwargs:
            kwargs['my_setting'] = load_env_value('MY_SETTING', 'default_value')
        super().__init__(**kwargs)
```

## Troubleshooting

### Common Issues

1. **Missing API Keys**: Check warning messages and set appropriate environment variables
2. **Invalid Port Numbers**: Ensure PORT is between 1 and 65535
3. **Path Issues**: Use absolute paths or ensure relative paths are correct
4. **Type Conversion Errors**: Check that environment variables contain valid values for their types

### Debugging Configuration
```python
from src.settings import inspect_settings
import json

# Print current configuration
config = inspect_settings()
print("Current configuration:")
print(json.dumps(config, indent=2, default=str))
```

### Resetting Configuration for Tests
```python
# Use the testing utility to avoid environment pollution
test_settings = get_settings_for_testing(
    # Override any settings needed for the test
    LLM_PROVIDER='ollama',
    LOG_LEVEL='DEBUG'
)
```

This robust settings system provides a solid foundation for configuration management while maintaining backward compatibility and offering extensive testing and debugging capabilities.