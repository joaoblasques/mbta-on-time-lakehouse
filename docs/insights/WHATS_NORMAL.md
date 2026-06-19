# WHATS_NORMAL.md — OTP Dreamer baseline

The OTP Dreamer's persistent memory of "what's normal" for the MBTA pipeline. Each dreaming pass
compares the day against this and **appends** refinements (additive + changelogged; never wholesale
overwrite — memory-drift guard). This is the "learned context" that makes idle learning possible.

## Baselines (as of 2026-06-19 · 1 day · 155k obs · on-time = −1..+5 min)
- **System OTP:** ~56% (treat <45% or >70% as worth investigating until more history accrues).
- **Median lateness:** ~+1.5 min · **avg:** ~+2.6 min.
- **Daily volume:** ~150k trip-stop observations, ~6k trips (full service day).

## Known patterns (established — don't re-flag as new anomalies)
- **Green Line light rail runs EARLY** by design/prediction (E-branch −4..−6 min; GLX Medford
  branch −7..−8 min). Low OTP here = *early*, not late.
- **Bridge St bus corridor runs chronically LATE** (~+10 min) — a standing hotspot.
- **Routes 236 / 240 / 350** are reliably *late* (median +4..+9 min).
- **Routes 8 / 23 / Mattapan** are reliably *early*.
- **OTP follows a daily curve:** higher AM (~64–67% at 06–07h), dips midday (~47% at 13h), partial PM recovery.

## Known data-quality caveats (don't treat as transit anomalies)
- Hours with low observation counts (e.g. early/late edges, <~5k obs) are **capture-window
  artifacts** when the poller wasn't running — exclude from OTP conclusions.
- OTP currently conflates early + late; prefer separate early/late breach rates once built.

## Changelog
- **2026-06-19** — baseline initialized from the first full day (manual Phase 0 dreaming pass).
