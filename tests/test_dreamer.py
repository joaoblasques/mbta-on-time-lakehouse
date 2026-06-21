"""Unit tests for the OTP Dreamer deterministic core (Phase 1) — no Spark, no network."""

from src.dreamer.baseline import OTP_BAND_MAX_MOVE, update_baseline
from src.dreamer.detect import detect_anomalies, split_findings
from src.dreamer.llm import LLMAnalyzer

TODAY = {
    "system": {"system_otp": 55.7},
    "worst_routes": [
        {"route_id": "236", "route_short_name": "236", "median_late_min": 9.0},   # late, new
        {"route_id": "8", "route_short_name": "8", "median_late_min": -4.1},       # early, known
        {"route_id": "99", "route_short_name": "99", "median_late_min": 1.0},      # neither
    ],
    "by_hour": [
        {"hour": 7, "otp_pct": 63.0, "obs": 25000},
        {"hour": 8, "otp_pct": 38.0, "obs": 3000},   # low volume → data-quality caveat
    ],
    "worst_stops": [
        {"stop_name": "Bridge St @ X", "median_late_min": 10.0, "otp_pct": 0.0},   # late stop
        {"stop_name": "Northeastern", "median_late_min": -5.0, "otp_pct": 2.0},     # early stop
    ],
}
BASELINE = {
    "system_otp_range": [50.0, 62.0],
    "known_late_routes": ["240"],
    "known_early_routes": ["8"],
    "min_hourly_obs": 5000,
}


def _by_kind(findings):
    out = {}
    for f in findings:
        out.setdefault(f.kind, []).append(f)
    return out


def test_late_vs_early_route_classification():
    k = _by_kind(detect_anomalies(TODAY, BASELINE))
    assert [f.subject for f in k["LATE_ROUTE"]] == ["236"]
    assert [f.subject for f in k["EARLY_ROUTE"]] == ["8"]
    # route 99 (median +1.0) is neither late nor early
    assert all(f.subject != "99" for fs in k.values() for f in fs)


def test_known_vs_new_tagging():
    k = _by_kind(detect_anomalies(TODAY, BASELINE))
    assert k["LATE_ROUTE"][0].is_known is False       # 236 not in baseline
    assert k["EARLY_ROUTE"][0].is_known is True        # 8 is a known early route


def test_low_volume_hour_is_data_quality_caveat():
    real, caveats = split_findings(detect_anomalies(TODAY, BASELINE))
    dq = [f for f in caveats if f.kind == "DATA_QUALITY"]
    assert dq and dq[0].subject == "hour 8" and dq[0].confidence == "low"
    assert all(f.kind != "DATA_QUALITY" for f in real)   # verify keeps DQ out of real anomalies


def test_stop_early_late_split():
    k = _by_kind(detect_anomalies(TODAY, BASELINE))
    assert k["LATE_STOP"][0].subject == "Bridge St @ X"
    assert k["EARLY_STOP"][0].subject == "Northeastern"


def test_system_shift_only_when_outside_band():
    assert not any(f.kind == "SYSTEM_SHIFT" for f in detect_anomalies(TODAY, BASELINE))
    shifted = {**TODAY, "system": {"system_otp": 30.0}}
    assert any(f.kind == "SYSTEM_SHIFT" for f in detect_anomalies(shifted, BASELINE))


def test_baseline_drift_guard_caps_band_move():
    b, _ = update_baseline(BASELINE, system_otp=90.0, late_routes=[], early_routes=[], date="2026-06-20")
    # high edge may move at most OTP_BAND_MAX_MOVE (62 → 68), not jump to 90
    assert b["system_otp_range"][1] == 62.0 + OTP_BAND_MAX_MOVE


def test_baseline_is_additive_and_capped():
    b, entry = update_baseline(BASELINE, system_otp=55.0,
                               late_routes=["236", "240", "x1", "x2", "x3"], early_routes=[],
                               date="2026-06-20")
    assert "240" in b["known_late_routes"]                 # existing never removed
    assert "236" in b["known_late_routes"]                 # new added
    added = [r for r in b["known_late_routes"] if r != "240"]
    assert len(added) <= 3                                  # capped per update
    assert b["changelog"][-1].startswith("2026-06-20")


def test_llm_analyzer_falls_back_when_api_errors(monkeypatch):
    real, caveats = split_findings(detect_anomalies(TODAY, BASELINE))
    a = LLMAnalyzer(api_key="x")
    monkeypatch.setattr(a, "_complete", lambda *args, **kw: (_ for _ in ()).throw(RuntimeError("rate limit")))
    out = a.narrate(real, caveats, TODAY["system"])
    assert "LLM unavailable" in out                 # graceful fallback message
    assert "Chronically late routes" in out         # deterministic narrative still present
    assert "236" in out                             # the real finding survives
