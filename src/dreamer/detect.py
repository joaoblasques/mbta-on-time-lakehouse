"""Anomaly detection over a day's gold OTP metrics, classified against the baseline.

Encodes the rules from the manual Phase 0 pass: low OTP splits into chronically-late vs
chronically-early; low-observation hours are capture artifacts (not transit anomalies);
findings are tagged known-vs-new against the baseline. Pure logic — unit-testable, no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

LATE_MIN = 5.0    # median minutes late to call a route/stop "late"
EARLY_MIN = -2.0  # median minutes to call it "early"


@dataclass
class Finding:
    kind: str          # LATE_ROUTE | EARLY_ROUTE | SYSTEM_SHIFT | DATA_QUALITY | LATE_STOP | EARLY_STOP
    subject: str
    value: float | None
    detail: str
    is_known: bool = False
    confidence: str = "high"   # "low" → treated as a caveat, not a real anomaly


def _name(route: dict) -> str:
    return route.get("route_short_name") or route.get("route_long_name") or route.get("route_id") or "?"


def detect_anomalies(today: dict, baseline: dict) -> list[Finding]:
    """Return findings for one day's metrics vs the baseline. `today` mirrors the gold peek
    shape: {system, worst_routes, best_routes, by_hour, worst_stops}."""
    out: list[Finding] = []

    # 1) system-level shift outside the learned band
    lo, hi = baseline.get("system_otp_range", [0.0, 100.0])
    sys_otp = (today.get("system") or {}).get("system_otp")
    if sys_otp is not None and (sys_otp < lo or sys_otp > hi):
        out.append(Finding("SYSTEM_SHIFT", "system_otp", sys_otp,
                           f"system OTP {sys_otp}% is outside the baseline band {lo}-{hi}%"))

    known_late = set(baseline.get("known_late_routes", []))
    known_early = set(baseline.get("known_early_routes", []))
    for r in today.get("worst_routes", []):
        med = r.get("median_late_min")
        if med is None:
            continue
        name, rid = _name(r), r.get("route_id")
        ident = {name, rid}
        if med >= LATE_MIN:
            out.append(Finding("LATE_ROUTE", name, med, f"route {name} median {med:+g} min late",
                               is_known=bool(ident & known_late)))
        elif med <= EARLY_MIN:
            out.append(Finding("EARLY_ROUTE", name, med, f"route {name} median {med:+g} min (early)",
                               is_known=bool(ident & known_early)))

    # 2) hourly capture artifacts (low volume) — flagged low-confidence
    min_obs = baseline.get("min_hourly_obs", 5000)
    for h in today.get("by_hour", []):
        if h.get("obs", 0) < min_obs:
            out.append(Finding("DATA_QUALITY", f"hour {h.get('hour')}", h.get("otp_pct"),
                               f"hour {h.get('hour')} OTP on low volume ({h.get('obs')} obs) "
                               f"— likely capture artifact", confidence="low"))

    # 3) worst stops, split early vs late
    for s in today.get("worst_stops", []):
        med = s.get("median_late_min")
        if med is None:
            continue
        kind = "LATE_STOP" if med > 0 else "EARLY_STOP"
        out.append(Finding(kind, s.get("stop_name"), med,
                           f"{s.get('stop_name')}: median {med:+g} min, OTP {s.get('otp_pct')}%"))
    return out


def split_findings(findings: list[Finding]) -> tuple[list[Finding], list[Finding]]:
    """Adversarial-verify split: (real anomalies, caveats). Low-confidence / data-quality
    findings are caveats, never reported as real transit anomalies."""
    real = [f for f in findings if f.confidence == "high" and f.kind != "DATA_QUALITY"]
    caveats = [f for f in findings if f.confidence != "high" or f.kind == "DATA_QUALITY"]
    return real, caveats
