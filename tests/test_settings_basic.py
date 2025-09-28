"""
Basic tests for the settings system without pytest dependency.
"""

import os
import warnings
from pathlib import Path
import tempfile

from src.settings import (
    Settings, DatabaseConfig, SQLConfig, LLMConfig, 
    load_env_value, get_settings_for_testing,
    inspect_settings
)


def test_load_env_value():
    """Test the load_env_value utility function."""
    print("Testing load_env_value...")
    
    # Test string value
    os.environ['TEST_VAR'] = 'test_value'
    assert load_env_value('TEST_VAR', 'default') == 'test_value'
    del os.environ['TEST_VAR']
    
    # Test default when missing
    assert load_env_value('NONEXISTENT_VAR', 'default') == 'default'
    
    # Test int value
    os.environ['TEST_INT'] = '42'
    assert load_env_value('TEST_INT', 0, int) == 42
    del os.environ['TEST_INT']
    
    # Test bool value
    os.environ['TEST_BOOL'] = 'true'
    assert load_env_value('TEST_BOOL', False, bool) is True
    del os.environ['TEST_BOOL']
    
    print("✓ load_env_value tests passed")


def test_database_config():
    """Test DatabaseConfig."""
    print("Testing DatabaseConfig...")
    
    # Test defaults
    config = DatabaseConfig()
    assert config.file_name == 'demo.duckdb'
    assert config.dir is None
    
    # Test env override
    os.environ['DB_FILE_NAME'] = 'custom.duckdb'
    os.environ['DB_DIR'] = '/custom/path'
    
    config = DatabaseConfig()
    assert config.file_name == 'custom.duckdb'
    assert config.dir == Path('/custom/path')
    
    del os.environ['DB_FILE_NAME']
    del os.environ['DB_DIR']
    
    print("✓ DatabaseConfig tests passed")


def test_sql_config():
    """Test SQLConfig."""
    print("Testing SQLConfig...")
    
    config = SQLConfig()
    assert config.row_limit == 200
    assert config.query_timeout_ms == 8000
    
    # Test validation
    try:
        SQLConfig(row_limit=0)
        assert False, "Should have raised validation error"
    except Exception as e:
        assert "Value must be positive" in str(e)
    
    print("✓ SQLConfig tests passed")


def test_llm_config():
    """Test LLMConfig."""
    print("Testing LLMConfig...")
    
    # Test warning for missing API key
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = LLMConfig()
        assert len(w) == 1
        assert "OPENAI_API_KEY is not set" in str(w[0].message)
    
    # Test no warning with API key
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = LLMConfig(openai_api_key='test-key')
        assert len(w) == 0
        assert config.openai_api_key == 'test-key'
    
    # Test no warning for ollama
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = LLMConfig(provider='ollama')
        assert len(w) == 0
    
    print("✓ LLMConfig tests passed")


def test_settings():
    """Test main Settings class."""
    print("Testing Settings...")
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        settings = Settings()
        
        # Test all sections exist
        assert hasattr(settings, 'database')
        assert hasattr(settings, 'sql')
        assert hasattr(settings, 'llm')
        assert hasattr(settings, 'server')
        assert hasattr(settings, 'logging')
        assert hasattr(settings, 'git')
        assert hasattr(settings, 'data_quality')
        assert hasattr(settings, 'data')
        assert hasattr(settings, 'orchestration')
        
        # Test config summary
        summary = settings.get_config_summary()
        assert 'database' in summary
        assert 'sql' in summary
        assert 'llm' in summary
        
        print("✓ Settings tests passed")


def test_utility_functions():
    """Test utility functions."""
    print("Testing utility functions...")
    
    # Test get_settings_for_testing
    test_settings = get_settings_for_testing(
        ROW_LIMIT='100',
        HOST='127.0.0.1'
    )
    
    assert test_settings.sql.row_limit == 100
    assert test_settings.server.host == '127.0.0.1'
    
    # Verify environment is restored
    assert os.environ.get('ROW_LIMIT') != '100'
    
    # Test inspect_settings
    summary = inspect_settings()
    assert isinstance(summary, dict)
    assert 'database' in summary
    
    print("✓ Utility functions tests passed")


def test_backward_compatibility():
    """Test backward compatibility."""
    print("Testing backward compatibility...")
    
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
    
    print("✓ Backward compatibility tests passed")


def test_environment_integration():
    """Test environment variable integration."""
    print("Testing environment integration...")
    
    env_vars = {
        'DB_FILE_NAME': 'test.duckdb',
        'ROW_LIMIT': '1000',
        'LLM_PROVIDER': 'ollama',
        'HOST': '192.168.1.100',
        'PORT': '9000',
        'LOG_LEVEL': 'DEBUG'
    }
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            settings = Settings()
            
            assert settings.database.file_name == 'test.duckdb'
            assert settings.sql.row_limit == 1000
            assert settings.llm.provider == 'ollama'
            assert settings.server.host == '192.168.1.100'
            assert settings.server.port == 9000
            assert settings.logging.level == 'DEBUG'
    
    finally:
        # Clean up environment
        for key in env_vars.keys():
            if key in os.environ:
                del os.environ[key]
    
    print("✓ Environment integration tests passed")


def main():
    """Run all tests."""
    print("Running basic settings tests...")
    print("=" * 50)
    
    try:
        test_load_env_value()
        test_database_config()
        test_sql_config()
        test_llm_config()
        test_settings()
        test_utility_functions()
        test_backward_compatibility()
        test_environment_integration()
        
        print("=" * 50)
        print("✅ All tests passed! The settings system is working correctly.")
        
    except Exception as e:
        print("=" * 50)
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    main()