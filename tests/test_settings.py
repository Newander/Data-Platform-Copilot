"""
Comprehensive tests for the robust settings configuration system.
"""

import os
import tempfile
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.settings import (
    Settings, DatabaseConfig, SQLConfig, LLMConfig, ServerConfig, 
    LoggingConfig, GitConfig, DataQualityConfig, DataConfig, 
    OrchestrationConfig, load_env_value, get_settings_for_testing,
    inspect_settings
)


class TestLoadEnvValue:
    """Test the load_env_value utility function."""
    
    def test_load_string_value(self):
        with patch.dict(os.environ, {'TEST_VAR': 'test_value'}):
            result = load_env_value('TEST_VAR', 'default')
            assert result == 'test_value'
    
    def test_load_default_when_missing(self):
        result = load_env_value('NONEXISTENT_VAR', 'default')
        assert result == 'default'
    
    def test_load_int_value(self):
        with patch.dict(os.environ, {'TEST_INT': '42'}):
            result = load_env_value('TEST_INT', 0, int)
            assert result == 42
    
    def test_load_float_value(self):
        with patch.dict(os.environ, {'TEST_FLOAT': '3.14'}):
            result = load_env_value('TEST_FLOAT', 0.0, float)
            assert result == 3.14
    
    def test_load_bool_value_true(self):
        with patch.dict(os.environ, {'TEST_BOOL': 'true'}):
            result = load_env_value('TEST_BOOL', False, bool)
            assert result is True
    
    def test_load_bool_value_false(self):
        with patch.dict(os.environ, {'TEST_BOOL': 'false'}):
            result = load_env_value('TEST_BOOL', True, bool)
            assert result is False
    
    def test_load_path_value(self):
        with patch.dict(os.environ, {'TEST_PATH': '/tmp/test'}):
            result = load_env_value('TEST_PATH', Path('/default'), Path)
            assert result == Path('/tmp/test')


class TestDatabaseConfig:
    """Test DatabaseConfig class."""
    
    def test_default_values(self):
        config = DatabaseConfig()
        assert config.file_name == 'demo.duckdb'
        assert config.dir is None
    
    def test_env_override(self):
        with patch.dict(os.environ, {'DB_FILE_NAME': 'custom.duckdb', 'DB_DIR': '/custom/path'}):
            config = DatabaseConfig()
            assert config.file_name == 'custom.duckdb'
            assert config.dir == Path('/custom/path')
    
    def test_direct_initialization(self):
        config = DatabaseConfig(file_name='direct.duckdb', dir=Path('/direct'))
        assert config.file_name == 'direct.duckdb'
        assert config.dir == Path('/direct')


class TestSQLConfig:
    """Test SQLConfig class."""
    
    def test_default_values(self):
        config = SQLConfig()
        assert config.row_limit == 200
        assert config.query_timeout_ms == 8000
    
    def test_validation_positive_values(self):
        with pytest.raises(ValidationError, match="Value must be positive"):
            SQLConfig(row_limit=0)
        
        with pytest.raises(ValidationError, match="Value must be positive"):
            SQLConfig(query_timeout_ms=-1)
    
    def test_env_override(self):
        with patch.dict(os.environ, {'ROW_LIMIT': '500', 'QUERY_TIMEOUT_MS': '15000'}):
            config = SQLConfig()
            assert config.row_limit == 500
            assert config.query_timeout_ms == 15000


class TestLLMConfig:
    """Test LLMConfig class."""
    
    def test_default_values(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Suppress API key warnings
            config = LLMConfig()
            assert config.provider == 'openai'
            assert config.model == 'gpt-4o-mini'
            assert config.openai_api_key is None
            assert config.openrouter_api_key is None
            assert config.ollama_base_url == 'http://localhost:11434'
    
    def test_api_key_warning_openai(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            LLMConfig()
            assert len(w) == 1
            assert "OPENAI_API_KEY is not set" in str(w[0].message)
    
    def test_api_key_warning_openrouter(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            LLMConfig(provider='openrouter')
            assert len(w) == 1
            assert "OPENROUTER_API_KEY is not set" in str(w[0].message)
    
    def test_no_warning_ollama(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            LLMConfig(provider='ollama')
            assert len(w) == 0
    
    def test_valid_with_api_key(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = LLMConfig(openai_api_key='test-key')
            assert len(w) == 0
            assert config.openai_api_key == 'test-key'


class TestServerConfig:
    """Test ServerConfig class."""
    
    def test_default_values(self):
        config = ServerConfig()
        assert config.host == '0.0.0.0'
        assert config.port == 8000
    
    def test_port_validation(self):
        with pytest.raises(ValidationError, match="Port must be between 1 and 65535"):
            ServerConfig(port=0)
        
        with pytest.raises(ValidationError, match="Port must be between 1 and 65535"):
            ServerConfig(port=70000)
    
    def test_valid_ports(self):
        config1 = ServerConfig(port=1)
        assert config1.port == 1
        
        config2 = ServerConfig(port=65535)
        assert config2.port == 65535


class TestLoggingConfig:
    """Test LoggingConfig class."""
    
    def test_default_values(self):
        config = LoggingConfig()
        assert config.level == 'INFO'
        assert config.format == '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
        assert config.datefmt == '%Y-%m-%d %H:%M:%S'
    
    def test_log_level_validation(self):
        with pytest.raises(ValidationError, match="Invalid log level"):
            LoggingConfig(level='INVALID')
    
    def test_valid_log_levels(self):
        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            config = LoggingConfig(level=level)
            assert config.level == level
        
        # Test case insensitive
        config = LoggingConfig(level='debug')
        assert config.level == 'DEBUG'


class TestGitConfig:
    """Test GitConfig class."""
    
    def test_default_values(self):
        config = GitConfig()
        assert config.dbt_dir == Path('dbt')
        assert config.github_token is None
        assert config.github_repo is None
        assert config.default_branch == 'main'
        assert config.author_name == 'Data Platform Copilot'
        assert config.author_email == 'bot@example.com'
    
    def test_github_repo_validation(self):
        with pytest.raises(ValidationError, match="GitHub repo must be in format 'owner/repo'"):
            GitConfig(github_repo='invalid-repo')
    
    def test_valid_github_repo(self):
        config = GitConfig(github_repo='owner/repo')
        assert config.github_repo == 'owner/repo'


class TestDataQualityConfig:
    """Test DataQualityConfig class."""
    
    def test_default_values(self):
        config = DataQualityConfig()
        assert config.default_limit == 10000
        assert config.max_limit == 200000
        assert config.default_sigma == 3.0
    
    def test_positive_value_validation(self):
        with pytest.raises(ValidationError, match="Value must be positive"):
            DataQualityConfig(default_limit=0)
        
        with pytest.raises(ValidationError, match="Sigma must be positive"):
            DataQualityConfig(default_sigma=-1.0)
    
    def test_limit_relationship_validation(self):
        with pytest.raises(ValidationError, match="default_limit cannot be greater than max_limit"):
            DataQualityConfig(default_limit=300000, max_limit=200000)


class TestSettings:
    """Test the main Settings class."""
    
    def test_settings_initialization(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            settings = Settings()
            
            assert isinstance(settings.database, DatabaseConfig)
            assert isinstance(settings.sql, SQLConfig)
            assert isinstance(settings.llm, LLMConfig)
            assert isinstance(settings.server, ServerConfig)
            assert isinstance(settings.logging, LoggingConfig)
            assert isinstance(settings.git, GitConfig)
            assert isinstance(settings.data_quality, DataQualityConfig)
            assert isinstance(settings.data, DataConfig)
            assert isinstance(settings.orchestration, OrchestrationConfig)
    
    def test_config_summary(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            settings = Settings()
            summary = settings.get_config_summary()
            
            assert 'database' in summary
            assert 'sql' in summary
            assert 'llm' in summary
            assert 'server' in summary
            assert 'logging' in summary
            assert 'git' in summary
            assert 'data_quality' in summary
            assert 'data' in summary
            assert 'orchestration' in summary
    
    def test_sensitive_data_masking(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            config_data = {
                'llm': LLMConfig(openai_api_key='secret-key'),
                'git': GitConfig(github_token='secret-token')
            }
            settings = Settings(**config_data)
            summary = settings.get_config_summary()
            
            assert summary['llm']['openai_api_key'] == '***masked***'
            assert summary['git']['github_token'] == '***masked***'


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_get_settings_for_testing(self):
        test_settings = get_settings_for_testing(
            OPENAI_API_KEY='test-key',
            ROW_LIMIT='100',
            HOST='127.0.0.1'
        )
        
        assert test_settings.llm.openai_api_key == 'test-key'
        assert test_settings.sql.row_limit == 100
        assert test_settings.server.host == '127.0.0.1'
        
        # Verify environment is restored
        assert os.environ.get('OPENAI_API_KEY') != 'test-key'
    
    def test_inspect_settings(self):
        summary = inspect_settings()
        assert isinstance(summary, dict)
        assert 'database' in summary
        assert 'llm' in summary
        # Should mask sensitive values
        if summary['llm']['openai_api_key']:
            assert summary['llm']['openai_api_key'] == '***masked***'


class TestBackwardCompatibility:
    """Test that module-level variables work for backward compatibility."""
    
    def test_module_level_variables(self):
        from src.settings import (
            DB_FILE_NAME, ROW_LIMIT, LLM_PROVIDER, SERVER_HOST, 
            SERVER_PORT, LOG_LEVEL, DBT_DIR
        )
        
        assert isinstance(DB_FILE_NAME, str)
        assert isinstance(ROW_LIMIT, int)
        assert isinstance(LLM_PROVIDER, str)
        assert isinstance(SERVER_HOST, str)
        assert isinstance(SERVER_PORT, int)
        assert isinstance(LOG_LEVEL, str)
        assert isinstance(DBT_DIR, Path)


class TestEnvironmentVariableIntegration:
    """Test environment variable integration."""
    
    def test_complex_environment_override(self):
        env_vars = {
            'DB_FILE_NAME': 'test.duckdb',
            'ROW_LIMIT': '1000',
            'LLM_PROVIDER': 'ollama',
            'LLM_MODEL': 'llama3',
            'HOST': '192.168.1.100',
            'PORT': '9000',
            'LOG_LEVEL': 'DEBUG',
            'DBT_DIR': '/custom/dbt',
            'DQ_DEFAULT_LIMIT': '5000'
        }
        
        with patch.dict(os.environ, env_vars):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                settings = Settings()
                
                assert settings.database.file_name == 'test.duckdb'
                assert settings.sql.row_limit == 1000
                assert settings.llm.provider == 'ollama'
                assert settings.llm.model == 'llama3'
                assert settings.server.host == '192.168.1.100'
                assert settings.server.port == 9000
                assert settings.logging.level == 'DEBUG'
                assert settings.git.dbt_dir == Path('/custom/dbt')
                assert settings.data_quality.default_limit == 5000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])