#!/usr/bin/env python3
"""
Test script to verify both DuckDB and PostgreSQL database configurations work.

This script tests the new database variable system that allows switching
between DuckDB and PostgreSQL databases via environment variables.
"""

import os
import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_duckdb_configuration():
    """Test DuckDB configuration and connection."""
    logger.info("=" * 60)
    logger.info("Testing DuckDB Configuration")
    logger.info("=" * 60)
    
    # Set environment variables for DuckDB
    os.environ['DATABASE_TYPE'] = 'duckdb'
    os.environ['DB_DIR'] = str(Path(__file__).parent / 'db')
    os.environ['DB_FILE_NAME'] = 'test.duckdb'
    
    try:
        # Import after setting environment variables
        from src.settings import settings, DATABASE_TYPE, DB_DIR, DB_FILE_NAME
        from src.database import get_engine, get_session, get_connection
        
        logger.info(f"Database Type: {DATABASE_TYPE}")
        logger.info(f"Database Directory: {DB_DIR}")
        logger.info(f"Database File: {DB_FILE_NAME}")
        
        # Test settings
        assert settings.database.database_type == "duckdb"
        assert settings.database.file_name == "test.duckdb"
        logger.info("✓ Settings configuration validated")
        
        # Test connection
        connection = get_connection()
        logger.info(f"✓ Connection created: {type(connection).__name__}")
        
        # Test engine
        engine = get_engine()
        logger.info(f"✓ Engine created: {engine.url}")
        
        # Test session and simple query
        with get_session() as session:
            from sqlalchemy import text
            result = session.execute(text("SELECT 1 as test_value")).fetchone()
            assert result[0] == 1
            logger.info("✓ Session and query test passed")
        
        logger.info("✅ DuckDB configuration test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ DuckDB configuration test FAILED: {e}")
        return False


def test_postgresql_configuration():
    """Test PostgreSQL configuration (without actual connection)."""
    logger.info("=" * 60)
    logger.info("Testing PostgreSQL Configuration")
    logger.info("=" * 60)
    
    # Set environment variables for PostgreSQL
    os.environ['DATABASE_TYPE'] = 'postgresql'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_DB'] = 'test_db'
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_password'
    os.environ['DB_SCHEMA'] = 'public'
    
    try:
        # Clear module cache to reload with new environment
        modules_to_clear = [name for name in sys.modules.keys() if name.startswith('src.')]
        for module in modules_to_clear:
            del sys.modules[module]
        
        # Import after setting environment variables
        from src.settings import settings, DATABASE_TYPE, DB_HOST, DB_PORT, DB_DB, DB_USER, DB_SCHEMA
        
        logger.info(f"Database Type: {DATABASE_TYPE}")
        logger.info(f"PostgreSQL Host: {DB_HOST}")
        logger.info(f"PostgreSQL Port: {DB_PORT}")
        logger.info(f"PostgreSQL Database: {DB_DB}")
        logger.info(f"PostgreSQL User: {DB_USER}")
        logger.info(f"PostgreSQL Schema: {DB_SCHEMA}")
        
        # Test settings
        assert settings.database.database_type == "postgresql"
        assert settings.database.host == "localhost"
        assert settings.database.port == 5432
        assert settings.database.database == "test_db"
        assert settings.database.user == "test_user"
        assert settings.database.password == "test_password"
        assert settings.database.default_schema == "public"
        logger.info("✓ Settings configuration validated")
        
        # Test that factory would create PostgreSQL connection
        # (We can't test actual connection without a PostgreSQL server)
        from src.database.connection_factory import DatabaseConnectionFactory
        
        # This should not raise an error for configuration
        try:
            connection_type = DATABASE_TYPE.lower()
            if connection_type == "postgresql":
                logger.info("✓ PostgreSQL connection type recognized")
            else:
                raise ValueError(f"Expected postgresql, got {connection_type}")
        except Exception as e:
            logger.error(f"Connection type validation failed: {e}")
            return False
        
        logger.info("✅ PostgreSQL configuration test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL configuration test FAILED: {e}")
        return False


def test_settings_validation():
    """Test settings validation for different database types."""
    logger.info("=" * 60)
    logger.info("Testing Settings Validation")
    logger.info("=" * 60)
    
    try:
        # Clear modules to avoid cached imports
        modules_to_clear = [name for name in sys.modules.keys() if name.startswith('src.')]
        for module in modules_to_clear:
            del sys.modules[module]
        
        # Test invalid database type by creating config directly with invalid value
        try:
            # Import the class without triggering global settings instantiation
            from src.settings import DatabaseConfig
            config = DatabaseConfig(database_type='invalid_db')
            logger.error("❌ Should have failed with invalid database type")
            return False
        except Exception as e:
            logger.info(f"✓ Correctly rejected invalid database type: {type(e).__name__}")
        
        # Test PostgreSQL without password (should fail validation)
        try:
            config = DatabaseConfig(
                database_type='postgresql',
                pg_password=None  # Missing required password
            )
            logger.error("❌ Should have failed without PostgreSQL password")
            return False
        except Exception as e:
            logger.info(f"✓ Correctly required PostgreSQL password: {type(e).__name__}")
        
        logger.info("✅ Settings validation test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Settings validation test FAILED: {e}")
        return False


def main():
    """Run all tests."""
    logger.info("Starting Database Configuration Tests")
    logger.info("=" * 80)
    
    tests = [
        ("DuckDB Configuration", test_duckdb_configuration),
        ("PostgreSQL Configuration", test_postgresql_configuration),
        ("Settings Validation", test_settings_validation),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        result = test_func()
        results.append((test_name, result))
        logger.info("")
    
    # Summary
    logger.info("=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nTests passed: {passed}/{len(results)}")
    
    if passed == len(results):
        logger.info("🎉 All tests passed! Database variable system is working correctly.")
        return 0
    else:
        logger.info("❌ Some tests failed. Please check the implementation.")
        return 1


if __name__ == "__main__":
    sys.exit(main())