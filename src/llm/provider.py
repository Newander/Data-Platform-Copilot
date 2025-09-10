import os
from typing import Optional
import httpx

PROVIDER = os.getenv("LLM_PROVIDER","none")

async def complete(prompt: str) -> str:
    if PROVIDER == "openai":
        from openai import AsyncOpenAI
        client = AsyncOpenAI()
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt}],
            temperature=0.1,
        )
        return resp.choices[0].message.content

    return ""
