from src.prompt import SYSTEM_PROMPT
from src.provider import complete
from src.schema_introspect import load_schema_docs


async def nl_to_sql(question: str, row_limit: int) -> str:
    prompt = SYSTEM_PROMPT.format(
        schema_docs=load_schema_docs()
    ).replace("{ROW_LIMIT}", str(row_limit))
    out = await complete(prompt + f"\nQ: {question}\nSQL:\n")
    return out
