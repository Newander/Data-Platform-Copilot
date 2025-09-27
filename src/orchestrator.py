# src/orchestrator.py
from typing import Dict, Any

import httpx

from src.settings import PREFECT_API


class OrchestratorError(RuntimeError):
    pass


async def run_flow(flow_name: str, params: Dict[str, Any] | None = None) -> dict:
    url = f"{PREFECT_API}/deployments/name/{flow_name}/schedule"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json={"parameters": params or {}})
        if r.status_code >= 300:
            raise OrchestratorError(f"Prefect run error {r.status_code}: {r.text}")
        return r.json()


async def get_status(flow_run_id: str) -> dict:
    url = f"{PREFECT_API}/flow_runs/{flow_run_id}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        if r.status_code >= 300:
            raise OrchestratorError(f"Prefect status error {r.status_code}: {r.text}")
        return r.json()
