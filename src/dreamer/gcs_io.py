"""Tiny GCS read/write for the dreamer's persistent artifacts (baseline + insight notes)."""

from __future__ import annotations

from google.cloud import storage


def read_text(bucket: str, path: str) -> str | None:
    blob = storage.Client().bucket(bucket).blob(path)
    return blob.download_as_text() if blob.exists() else None


def write_text(bucket: str, path: str, text: str, content_type: str = "text/markdown") -> None:
    storage.Client().bucket(bucket).blob(path).upload_from_string(text, content_type=content_type)
