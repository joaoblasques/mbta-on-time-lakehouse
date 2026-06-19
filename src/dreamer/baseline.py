"""Baseline ("what's normal") memory for the OTP Dreamer — load, save, and drift-guarded update.

This is the persistent "learned context" that makes idle learning possible. Updates are
**additive and bounded** (memory-drift guard): the OTP band may only move so far per pass, and
known-pattern lists grow (capped) but are never wiped. Pure logic + small file I/O.
"""

from __future__ import annotations

import json

OTP_BAND_MAX_MOVE = 6.0   # a band edge may move at most this many points per update (drift guard)
MAX_NEW_ROUTES = 3        # at most this many newly-learned routes added per side, per update


def load_baseline(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def save_baseline(baseline: dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(baseline, f, indent=2)
        f.write("\n")


def update_baseline(baseline: dict, *, system_otp: float | None,
                    late_routes: list[str], early_routes: list[str], date: str) -> tuple[dict, str]:
    """Return (new_baseline, changelog_entry). Additive + drift-guarded; the caller decides
    whether to persist (e.g. only on human approval)."""
    b = json.loads(json.dumps(baseline))  # deep copy; never mutate the input
    changes: list[str] = []

    if system_otp is not None:
        lo, hi = b.get("system_otp_range", [system_otp, system_otp])
        if system_otp < lo:
            lo = max(system_otp, lo - OTP_BAND_MAX_MOVE)   # widen down, capped
            changes.append(f"otp band low → {lo:g}")
        elif system_otp > hi:
            hi = min(system_otp, hi + OTP_BAND_MAX_MOVE)   # widen up, capped
            changes.append(f"otp band high → {hi:g}")
        b["system_otp_range"] = [round(lo, 1), round(hi, 1)]

    def _add(key: str, routes: list[str]) -> None:
        known = b.setdefault(key, [])
        new = [r for r in routes if r and r not in known][:MAX_NEW_ROUTES]
        if new:
            known.extend(new)
            changes.append(f"{key} += {new}")

    _add("known_late_routes", late_routes)
    _add("known_early_routes", early_routes)

    entry = f"{date}: " + ("; ".join(changes) if changes else "no baseline change")
    b.setdefault("changelog", []).append(entry)
    return b, entry
