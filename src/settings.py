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
