import httpx

from src.config import OPENAI_API_KEY, LLM_MODEL, OPENROUTER_API_KEY, OLLAMA_BASE_URL, LLM_PROVIDER

# General defaults for more deterministic SQL generation
GEN_PARAMS = {
    "temperature": 0.05,
    "top_p": 0.9,
    "max_tokens": 800,  # SQL + explanations (if there are any)
}


class LLMError(RuntimeError):
    pass


async def openai_complete(system_prompt: str, user_prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise LLMError("OPENAI_API_KEY is not set")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": GEN_PARAMS["temperature"],
        "top_p": GEN_PARAMS["top_p"],
        "max_tokens": GEN_PARAMS["max_tokens"],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code >= 300:
            raise LLMError(f"OpenAI error {r.status_code}: {r.text}")
        data = r.json()
        return data["choices"][0]["message"]["content"]


async def openrouter_complete(system_prompt: str, user_prompt: str) -> str:
    if not OPENROUTER_API_KEY:
        raise LLMError("OPENROUTER_API_KEY is not set")

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": "https://github.com/yourname/data-platform-copilot",
        "X-Title": "Data Platform Copilot",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,  # e.g. "meta-llama/llama-3.1-70b-instruct:free"
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": GEN_PARAMS["temperature"],
        "top_p": GEN_PARAMS["top_p"],
        "max_tokens": GEN_PARAMS["max_tokens"],
    }
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code >= 300:
            raise LLMError(f"OpenRouter error {r.status_code}: {r.text}")
        data = r.json()
        return data["choices"][0]["message"]["content"]


async def ollama_complete(system_prompt: str, user_prompt: str) -> str:
    url = f"{OLLAMA_BASE_URL}/v1/chat/completions"  # Ollama's compatible endpoint >= v0.3
    payload = {
        "model": LLM_MODEL,  # e.g. "llama3.1"
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": GEN_PARAMS["temperature"],
        "top_p": GEN_PARAMS["top_p"],
        "max_tokens": GEN_PARAMS["max_tokens"],
        "stream": False,
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(url, json=payload)
        if r.status_code >= 300:
            raise LLMError(f"Ollama error {r.status_code}: {r.text}")
        data = r.json()
        # Ollama chat/completions responds in a format similar to OpenAI
        # If using /api/chat (the old one), adapt the parsing.
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            # Fallback для /api/chat
            # data = {"message":{"content": "..."}}
            if isinstance(data, dict) and "message" in data and "content" in data["message"]:
                return data["message"]["content"]
            raise LLMError(f"Ollama unexpected response: {data}")


async def complete(system_prompt: str, user_prompt: str) -> str:
    if LLM_PROVIDER == "openai":
        return await openai_complete(system_prompt, user_prompt)
    if LLM_PROVIDER == "openrouter":
        return await openrouter_complete(system_prompt, user_prompt)
    if LLM_PROVIDER == "ollama":
        return await ollama_complete(system_prompt, user_prompt)
    raise LLMError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")
