"""Read the gold OTP marts via the Databricks SQL Statement Execution API (no notebook job).

Returns the `today` metrics dict that detect/narrate consume. Pure REST — runs from anywhere
with the workspace host + a PAT + a serverless SQL warehouse id.
"""

from __future__ import annotations

import time

import requests

_PATH = "/api/2.0/sql/statements"


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return v


def _run_sql(host: str, token: str, warehouse_id: str, sql: str, timeout: int = 60) -> list[dict]:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.post(f"{host}{_PATH}", headers=h, timeout=timeout, json={
        "statement": sql, "warehouse_id": warehouse_id,
        "wait_timeout": "30s", "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY"})
    r.raise_for_status()
    d = r.json()
    sid = d["statement_id"]
    while d["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = requests.get(f"{host}{_PATH}/{sid}", headers=h, timeout=timeout).json()
    if d["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({sql[:40]}…): {d['status']}")
    cols = [c["name"] for c in d["manifest"]["schema"]["columns"]]
    rows = (d.get("result") or {}).get("data_array") or []
    return [{c: _num(v) for c, v in zip(cols, row)} for row in rows]


def fetch_gold(host: str, token: str, warehouse_id: str) -> dict:
    def q(sql):
        return _run_sql(host, token, warehouse_id, sql)

    system = q(
        "SELECT count(*) AS lateness_rows, "
        "round(100.0*sum(case when lateness_min between -1 and 5 then 1 else 0 end)/count(*),1) AS system_otp, "
        "round(avg(lateness_min),1) AS avg_late, round(percentile(lateness_min,0.5),1) AS median_late "
        "FROM mbta.silver.trip_stop_lateness")
    return {
        "system": system[0] if system else {},
        "worst_routes": q("SELECT coalesce(route_short_name,route_long_name) AS route_short_name, "
                          "route_id, observations, otp_pct, avg_late_min, median_late_min "
                          "FROM mbta.gold.otp_by_route WHERE observations>=50 ORDER BY otp_pct ASC LIMIT 15"),
        "best_routes": q("SELECT coalesce(route_short_name,route_long_name) AS route_short_name, otp_pct, observations "
                         "FROM mbta.gold.otp_by_route WHERE observations>=50 ORDER BY otp_pct DESC LIMIT 8"),
        "by_hour": q("SELECT hour, round(100.0*sum(on_time_n)/sum(observations),1) AS otp_pct, "
                     "sum(observations) AS obs FROM mbta.gold.otp_by_route_hour GROUP BY hour ORDER BY hour"),
        "worst_stops": q("SELECT stop_name, otp_pct, observations, median_late_min "
                         "FROM mbta.gold.otp_by_stop WHERE observations>=20 ORDER BY otp_pct ASC LIMIT 20"),
    }
