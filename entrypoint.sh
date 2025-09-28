#!/usr/bin/env sh
set -e

# =============================================================================
# Data Pilot Application Configuration
# =============================================================================

# Server Configuration
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8080}"
export WORKERS="${WORKERS:-1}"

# Database Configuration
export DB_FILE_NAME="${DB_FILE_NAME:-demo.duckdb}"
export DB_DIR="${DB_DIR:-}"

# SQL Configuration
export ROW_LIMIT="${ROW_LIMIT:-200}"
export QUERY_TIMEOUT_MS="${QUERY_TIMEOUT_MS:-8000}"

# LLM Configuration
export LLM_PROVIDER="${LLM_PROVIDER:-openai}"
export LLM_MODEL="${LLM_MODEL:-gpt-4o-mini}"
export OPENAI_API_KEY="${OPENAI_API_KEY:-}"
export OPENROUTER_API_KEY="${OPENROUTER_API_KEY:-}"
export OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434}"

# Logging Configuration
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export LOG_FORMAT="${LOG_FORMAT:-%(asctime)s | %(levelname)s | %(name)s | %(message)s}"
export LOG_DATEFMT="${LOG_DATEFMT:-%Y-%m-%d %H:%M:%S}"

# Git/DBT Configuration
export DBT_DIR="${DBT_DIR:-dbt}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-}"
export GITHUB_REPO="${GITHUB_REPO:-}"
export GIT_DEFAULT_BRANCH="${GIT_DEFAULT_BRANCH:-main}"
export GIT_AUTHOR_NAME="${GIT_AUTHOR_NAME:-Data Platform Copilot}"
export GIT_AUTHOR_EMAIL="${GIT_AUTHOR_EMAIL:-bot@example.com}"

# Data Quality Configuration
export DQ_DEFAULT_LIMIT="${DQ_DEFAULT_LIMIT:-10000}"
export DQ_MAX_LIMIT="${DQ_MAX_LIMIT:-200000}"
export DQ_DEFAULT_SIGMA="${DQ_DEFAULT_SIGMA:-3.0}"

# Data Configuration
export DATA_DIR="${DATA_DIR:-}"

# Orchestration Configuration
export PREFECT_API="${PREFECT_API:-http://localhost:4200/api}"

# Prefect Worker Configuration (for docker-compose)
export PREFECT_API_URL="${PREFECT_API_URL:-http://prefect:4200/api}"
export PREFECT_LOGGING_LEVEL="${PREFECT_LOGGING_LEVEL:-INFO}"
export WORK_QUEUE="${WORK_QUEUE:-default}"

# Python Configuration
export PYTHONPATH="${PYTHONPATH:-/src}"

# =============================================================================
# Application Startup
# =============================================================================

echo "=== Data Pilot Application Starting ==="
echo "Host: $HOST"
echo "Port: $PORT"
echo "Workers: $WORKERS"
echo "LLM Provider: $LLM_PROVIDER"
echo "LLM Model: $LLM_MODEL"
echo "Log Level: $LOG_LEVEL"
echo "Database File: $DB_FILE_NAME"
echo "DBT Directory: $DBT_DIR"
echo "========================================"

exec granian --interface asgi --host "$HOST" --port "$PORT" --workers "$WORKERS" main:app