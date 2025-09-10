from src.agent.prompt import SYSTEM_PROMPT
from src.llm.provider import complete
from src.tools.schema_introspect import load_schema_docs


async def nl_to_sql(question: str, row_limit: int) -> str:
    prompt = SYSTEM_PROMPT.format(
        schema_docs=load_schema_docs()
    ).replace("{ROW_LIMIT}", str(row_limit))
    out = await complete(prompt + f"\nQ: {question}\nSQL:\n")
    return out
