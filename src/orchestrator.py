# src/orchestrator.py
import os
from typing import Dict, Any, Optional

import httpx

PREFECT_API = os.getenv("PREFECT_API", "http://localhost:4200/api")


class OrchestratorError(RuntimeError):
    pass


async def _raise_for_status(r: httpx.Response):
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        # подробная ошибка в логах и наверх
        raise OrchestratorError(f"{r.request.method} {r.request.url} -> {r.status_code}: {r.text}") from e


async def run_flow(
        flow_name: str,
        deployment_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Prefect 2: правильный старт через create_flow_run.
    Если deployment_name не передан, пытаемся найти первый подходящий.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        if not deployment_name:
            # авто-поиск деплоймента по имени флоу
            url = f"{PREFECT_API}/deployments/filter"
            payload = {
                "offset": 0, "limit": 20,
                "sort": "DESC",
                "deployments": {"name_like": None},
                "flows": {"name": {"any_": [flow_name]}},
            }
            r = await client.post(url, json=payload)
            await _raise_for_status(r)
            items = r.json()
            if not items:
                raise OrchestratorError(f"No deployments found for flow '{flow_name}'. "
                                        f"Проверь, что ты сделал 'deployment build … --apply'.")
            deployment_name = items[0]["name"]  # например, daily_sales_depl

        # старт ран-а
        url = f"{PREFECT_API}/deployments/name/{flow_name}/{deployment_name}/create_flow_run"
        r = await client.post(url, json={"parameters": params or {}})
        await _raise_for_status(r)
        return r.json()


async def get_status(flow_run_id: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        url = f"{PREFECT_API}/flow_runs/{flow_run_id}"
        r = await client.get(url)
        await _raise_for_status(r)
        return r.json()
