import os
from pathlib import Path

# Constants
DB_FILE_NAME = "demo.duckdb"

# SQL Parameters
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "200"))
QUERY_TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "8000"))
# AI vars
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
# Server
SERVER_HOST = os.getenv("HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("PORT", "8000"))
# Data settings
DB_DIR = Path(os.getenv("DB_DIR"))
DATA_DIR = Path(os.getenv("DATA_DIR"))
# Logs
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
DATE_FORMAT = os.getenv("LOG_DATEFMT", "%Y-%m-%d %H:%M:%S")
# dbt & Git
DBT_DIR = Path(os.getenv("DBT_DIR", "dbt"))  # project root dbt
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # format "owner/repo"
GIT_DEFAULT_BRANCH = os.getenv("GIT_DEFAULT_BRANCH", "main")
GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "Data Platform Copilot")
GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "bot@example.com")# в конец файла
# Data Quality
DQ_DEFAULT_LIMIT = int(os.getenv("DQ_DEFAULT_LIMIT", "10000"))  # сколько строк тянуть на профиль
DQ_MAX_LIMIT = int(os.getenv("DQ_MAX_LIMIT", "200000"))  # предохранитель
DQ_DEFAULT_SIGMA = float(os.getenv("DQ_DEFAULT_SIGMA", "3.0"))  # для z-score

