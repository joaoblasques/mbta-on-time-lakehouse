"""OTP Dreamer entrypoint (Phase 2). Runs read-only: pull gold via the Databricks SQL API →
load baseline from GCS → detect + verify + LLM-narrate (OpenRouter) → write the insight note
+ updated baseline back to GCS. Never modifies the pipeline.

Env (Cloud Run injects the secret ones): DATABRICKS_HOST, DATABRICKS_TOKEN,
DATABRICKS_WAREHOUSE_ID, GCS_BUCKET, OPENROUTER_API_KEY, OPENROUTER_MODEL (optional).

Run:  python -m src.dreamer.run_dreamer
"""

from __future__ import annotations

import datetime as dt
import json
import os

from . import dream
from .gcs_io import read_text, write_text
from .gold_client import fetch_gold
from .llm import LLMAnalyzer

DEFAULT_BASELINE = {
    "system_otp_range": [40.0, 70.0],
    "known_late_routes": [], "known_early_routes": [],
    "min_hourly_obs": 5000, "changelog": [],
}


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]
    bucket = os.environ["GCS_BUCKET"]
    model = os.environ.get("OPENROUTER_MODEL", "openai/gpt-oss-120b:free")
    date = dt.date.today().isoformat()

    metrics = fetch_gold(host, token, warehouse)
    base_txt = read_text(bucket, "_dreamer/baseline.json")
    baseline = json.loads(base_txt) if base_txt else DEFAULT_BASELINE

    analyzer = LLMAnalyzer(os.environ["OPENROUTER_API_KEY"], model=model)
    note, new_baseline, changelog = dream.run(metrics, baseline, date, analyzer=analyzer)

    write_text(bucket, f"_dreamer/insights/dt={date}/insight.md", note)
    write_text(bucket, "_dreamer/baseline.json", json.dumps(new_baseline, indent=2), "application/json")
    print(f"dreamer OK → gs://{bucket}/_dreamer/insights/dt={date}/insight.md | baseline: {changelog}")


if __name__ == "__main__":
    main()
