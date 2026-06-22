"""Open a CI-gated PR via the GitHub REST API — the Tier-2 "auto-propose" feedback channel.

The dreamer's insight + proposed marts enter the repo as a PR a human reviews/merges (never
auto-merged). Pure REST (uses `requests`). Idempotent-ish: re-running the same day reuses the
day's branch and updates the file; if a PR already exists it returns its URL.
"""

from __future__ import annotations

import base64

import requests

API = "https://api.github.com"


def _gh(method: str, token: str, path: str, **kw) -> dict:
    r = requests.request(method, f"{API}{path}", timeout=30, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, **kw)
    r.raise_for_status()
    return r.json() if r.text else {}


def open_issue(token: str, repo: str, title: str, body: str) -> str:
    return _gh("POST", token, f"/repos/{repo}/issues", json={"title": title, "body": body})["html_url"]


def find_open_issue(token: str, repo: str, title: str) -> str | None:
    """URL of an open issue with this exact title (skips PRs), else None — for dedupe."""
    issues = _gh("GET", token, f"/repos/{repo}/issues?state=open&per_page=50")
    return next((i["html_url"] for i in issues
                 if i.get("title") == title and "pull_request" not in i), None)


def _existing_pr_url(token: str, repo: str, branch: str, base: str) -> str | None:
    owner = repo.split("/")[0]
    prs = _gh("GET", token, f"/repos/{repo}/pulls?head={owner}:{branch}&base={base}&state=open")
    return prs[0]["html_url"] if prs else None


def open_pr(token: str, repo: str, branch: str, files: dict[str, str],
            title: str, body: str, base: str = "main") -> str:
    """Create/reuse `branch`, write `files` {path: content}, open (or reuse) a PR. Returns URL."""
    base_sha = _gh("GET", token, f"/repos/{repo}/git/ref/heads/{base}")["object"]["sha"]
    try:
        _gh("POST", token, f"/repos/{repo}/git/refs",
            json={"ref": f"refs/heads/{branch}", "sha": base_sha})
    except requests.HTTPError as e:
        if e.response.status_code != 422:  # 422 = branch already exists → reuse it
            raise

    for path, content in files.items():
        sha = None
        try:
            sha = _gh("GET", token, f"/repos/{repo}/contents/{path}?ref={branch}").get("sha")
        except requests.HTTPError:
            pass  # file doesn't exist on the branch yet
        payload = {"message": f"dreamer: update {path}", "branch": branch,
                   "content": base64.b64encode(content.encode()).decode()}
        if sha:
            payload["sha"] = sha
        _gh("PUT", token, f"/repos/{repo}/contents/{path}", json=payload)

    try:
        pr = _gh("POST", token, f"/repos/{repo}/pulls",
                 json={"title": title, "head": branch, "base": base, "body": body})
        return pr["html_url"]
    except requests.HTTPError as e:
        if e.response.status_code == 422:  # PR already open for this branch
            return _existing_pr_url(token, repo, branch, base) or "(PR already open)"
        raise
