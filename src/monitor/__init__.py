"""Failure-monitor: the self-healing half of the self-managing loop.

Reads the medallion job's recent run history and acts by tier: a fresh failure → auto-retry
(Tier-1); persistent failures (retry exhausted) → open a deduped GitHub issue (Tier-2). Never
auto-fixes anything risky. Read-mostly; the only write to the pipeline is re-triggering a run.
"""
