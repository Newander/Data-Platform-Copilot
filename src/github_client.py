import base64

import httpx

from src.config import (
    GITHUB_TOKEN, GITHUB_REPO, GIT_DEFAULT_BRANCH,
    GIT_AUTHOR_NAME, GIT_AUTHOR_EMAIL,
)


class GitHubError(RuntimeError):
    pass


def _headers():
    if not GITHUB_TOKEN:
        raise GitHubError("GITHUB_TOKEN is not set")
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }


def _api(path: str) -> str:
    if not GITHUB_REPO:
        raise GitHubError("GITHUB_REPO is not set (expected 'owner/repo')")
    return f"https://api.github.com/repos/{GITHUB_REPO}{path}"


async def get_branch_sha(branch: str) -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_api(f"/git/ref/heads/{branch}"), headers=_headers())
        if r.status_code == 404:
            raise GitHubError(f"Branch not found: {branch}")
        r.raise_for_status()
        return r.json()["object"]["sha"]


async def create_branch(new_branch: str, from_branch: str | None = None) -> str:
    base = from_branch or GIT_DEFAULT_BRANCH
    sha = await get_branch_sha(base)
    payload = {"ref": f"refs/heads/{new_branch}", "sha": sha}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(_api("/git/refs"), headers=_headers(), json=payload)
        if r.status_code not in (200, 201, 422):  # 422 if already exists
            raise GitHubError(f"Create branch failed: {r.status_code} {r.text}")
        if r.status_code == 422:
            # already exists → just return sha
            return await get_branch_sha(new_branch)
        return r.json()["object"]["sha"]


async def get_file_sha_if_exists(path: str, branch: str) -> str | None:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_api(f"/contents/{path}"), headers=_headers(), params={"ref": branch})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("sha")


async def upsert_file(path: str, content: str, branch: str, message: str) -> dict:
    b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    sha = await get_file_sha_if_exists(path, branch)
    payload = {
        "message": message,
        "content": b64,
        "branch": branch,
        "committer": {"name": GIT_AUTHOR_NAME, "email": GIT_AUTHOR_EMAIL},
    }
    if sha:
        payload["sha"] = sha
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(_api(f"/contents/{path}"), headers=_headers(), json=payload)
        if r.status_code not in (200, 201):
            raise GitHubError(f"Upsert file failed: {r.status_code} {r.text}")
        return r.json()


async def create_pull_request(title: str, head: str, base: str | None = None, body: str | None = None) -> dict:
    payload = {"title": title, "head": head, "base": base or GIT_DEFAULT_BRANCH}
    if body:
        payload["body"] = body
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(_api("/pulls"), headers=_headers(), json=payload)
        if r.status_code not in (200, 201):
            raise GitHubError(f"Create PR failed: {r.status_code} {r.text}")
        return r.json()
