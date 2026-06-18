"""Smoke test so CI is green from commit 1. Real tests arrive with Phase 1 transforms."""


def test_repo_imports():
    import src.ingestion  # noqa: F401

    assert True
