import os

PROVIDER = os.getenv("LLM_PROVIDER", "none")


async def complete(prompt: str) -> str:
    if PROVIDER == "openai":
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEI"))
        resp = await client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "system", "content": prompt, "name": "analytical question"}],
            temperature=0.1,
        )
        return resp.choices[0].message.content

    return ""
