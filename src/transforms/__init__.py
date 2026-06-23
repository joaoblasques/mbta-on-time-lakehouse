"""Pure pyspark transforms for the medallion — the single source of truth for the silver/gold
logic, imported by both the Databricks notebooks and the integration tests (so tests exercise the
*real* code, not a copy). No I/O, no Unity Catalog, no dbutils — just DataFrame → DataFrame.
"""
