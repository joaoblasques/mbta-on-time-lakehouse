"""Phase-1 orchestrator: gold metrics + baseline → findings → verify split → insight note.

Ships a *deterministic* renderer so the dreamer runs end-to-end today (no API key). The LLM
"brain" is the `Analyzer` seam: Phase 2 supplies a ClaudeAnalyzer that enriches the narrative
and proposes novel marts. Read-only — returns artifacts; the caller decides what to persist.
"""

from __future__ import annotations

from typing import Protocol

from .baseline import update_baseline
from .detect import Finding, detect_anomalies, split_findings


class Analyzer(Protocol):
    """Phase-2 seam. A ClaudeAnalyzer will implement this with an API call + token budget."""
    def narrate(self, real: list[Finding], caveats: list[Finding], system: dict) -> str: ...


def _section(title: str, items: list[Finding]) -> str:
    if not items:
        return ""
    lines = [f"### {title}"]
    lines += [f"- {f.detail}" + (" *(known)*" if f.is_known else " **(new)**") for f in items]
    return "\n".join(lines) + "\n"


def deterministic_narrative(real: list[Finding], caveats: list[Finding], system: dict) -> str:
    """Templated fallback narrative (Phase 1). Phase 2's ClaudeAnalyzer replaces this."""
    by = {}
    for f in real:
        by.setdefault(f.kind, []).append(f)
    parts = [f"System OTP **{system.get('system_otp')}%** "
             f"(median {system.get('median_late')}, avg {system.get('avg_late')} min)."]
    parts += [
        _section("Chronically late routes", by.get("LATE_ROUTE", [])),
        _section("Chronically early routes", by.get("EARLY_ROUTE", [])),
        _section("Late stops", by.get("LATE_STOP", [])),
        _section("Early stops", by.get("EARLY_STOP", [])),
        _section("System shift", by.get("SYSTEM_SHIFT", [])),
    ]
    if caveats:
        parts.append("### Data-quality caveats (excluded from anomalies)\n"
                     + "\n".join(f"- {f.detail}" for f in caveats) + "\n")
    return "\n".join(p for p in parts if p)


def run(metrics: dict, baseline: dict, date: str, analyzer: Analyzer | None = None):
    """Return (note_markdown, new_baseline, changelog_entry). Persists nothing."""
    findings = detect_anomalies(metrics, baseline)
    real, caveats = split_findings(findings)
    narrate = analyzer.narrate if analyzer else deterministic_narrative
    note = f"# OTP Insight — {date}\n\n*OTP Dreamer (read-only).*\n\n" + narrate(real, caveats, metrics.get("system", {}))
    late = [f.subject for f in real if f.kind == "LATE_ROUTE"]
    early = [f.subject for f in real if f.kind == "EARLY_ROUTE"]
    new_baseline, changelog = update_baseline(
        baseline, system_otp=(metrics.get("system") or {}).get("system_otp"),
        late_routes=late, early_routes=early, date=date)
    return note, new_baseline, changelog
