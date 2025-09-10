import os


def load_schema_docs() -> str:
    with open(f"{os.getenv("DB_DIR")}/schema_docs.md","r",encoding="utf-8") as f:
        return f.read()
