"""OTP Dreamer — sleep-time analysis of the MBTA gold OTP marts.

Deterministic core (Phase 1): anomaly detection + drift-guarded baseline memory + a verify
split. The LLM "brain" (narrative insight + novel-mart proposals) plugs in at Phase 2 via the
Analyzer seam in `dream.py`. Runs read-only — it proposes, it never changes the pipeline.
"""
