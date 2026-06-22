"""Failure-monitor entrypoint — the self-healing half of the self-managing loop.

Reads the medallion job's recent run history and acts by tier:
  - **Tier 1 (auto):** a *fresh* failure → re-trigger the job once (bounded by RETRY_LIMIT).
  - **Tier 2 (auto-propose):** failures past the retry limit → open a *deduped* GitHub issue.
Never auto-fixes anything risky. The only pipeline write is re-running a job.

Env: DATABRICKS_HOST, DATABRICKS_TOKEN, MEDALLION_JOB_ID, GITHUB_TOKEN, GITHUB_REPO,
RETRY_LIMIT (default 1).
"""

from __future__ import annotations

import os

import requests

from src.dreamer.github_pr import find_open_issue, open_issue

ISSUE_TITLE = "[monitor] medallion-refresh runs failing"


def _inflight(r: dict) -> bool:
    return r.get("state", {}).get("life_cycle_state") in ("PENDING", "RUNNING", "QUEUED", "BLOCKED")


def _success(r: dict) -> bool:
    return r.get("state", {}).get("result_state") == "SUCCESS"


def _failure(r: dict) -> bool:
    s = r.get("state", {})
    return s.get("life_cycle_state") == "INTERNAL_ERROR" or s.get("result_state") in ("FAILED", "TIMEDOUT")


def decide(runs: list[dict], retry_limit: int = 1) -> str:
    """runs newest-first → 'ok' | 'retry' | 'escalate'. Pure (testable, no I/O)."""
    if not runs or _inflight(runs[0]):
        return "ok"                       # a run is in flight or nothing terminal yet
    fails = 0
    for r in runs:
        if _inflight(r):
            continue
        if _success(r):
            break
        if _failure(r):
            fails += 1
        else:
            break                          # canceled/other terminal ends the streak
    if fails == 0:
        return "ok"
    return "retry" if fails <= retry_limit else "escalate"


def _dbx(host: str, token: str, method: str, path: str, **kw) -> dict:
    r = requests.request(method, f"{host}{path}", timeout=30,
                         headers={"Authorization": f"Bearer {token}"}, **kw)
    r.raise_for_status()
    return r.json() if r.text else {}


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    job_id = os.environ["MEDALLION_JOB_ID"]
    retry_limit = int(os.environ.get("RETRY_LIMIT", "1"))

    runs = _dbx(host, token, "GET", f"/api/2.1/jobs/runs/list?job_id={job_id}&limit=10").get("runs", [])
    action = decide(runs, retry_limit)

    if action == "retry":
        _dbx(host, token, "POST", "/api/2.1/jobs/run-now", json={"job_id": int(job_id)})
        print(f"monitor: Tier-1 auto-retry of medallion job {job_id} (fresh failure)")
    elif action == "escalate":
        gh = os.environ.get("GITHUB_TOKEN")
        repo = os.environ.get("GITHUB_REPO", "joaoblasques/mbta-on-time-lakehouse")
        if gh and not find_open_issue(gh, repo, ISSUE_TITLE):
            s = runs[0].get("state", {})
            body = (f"`mbta-medallion-refresh` has failed past the auto-retry limit ({retry_limit}).\n\n"
                    f"Latest run: {s.get('life_cycle_state')} {s.get('result_state', '')} — "
                    f"{str(s.get('state_message', ''))[:300]}\n\nAuto-filed by `mbta-monitor` (Tier-2).")
            print(f"monitor: Tier-2 escalation → {open_issue(gh, repo, ISSUE_TITLE, body)}")
        else:
            print("monitor: escalation needed but issue already open (or no token)")
    else:
        print("monitor: ok — latest run healthy or in-flight")


if __name__ == "__main__":
    main()
