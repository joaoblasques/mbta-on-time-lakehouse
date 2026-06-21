"""LLMAnalyzer — the Phase-2 "brain". Narrates findings + proposes novel marts via OpenRouter
(OpenAI-compatible). Token-budgeted; falls back to the deterministic narrative on ANY error
(rate-limit, network, bad key) so a dreamer run never fails because of the LLM. Read-only.

The deterministic findings are always appended after the LLM prose, so the *authoritative*
facts are present even if the model is unavailable or drifts.
"""

from __future__ import annotations

from .detect import Finding
from .dream import deterministic_narrative

SYSTEM_PROMPT = (
    "You are a senior data engineer reviewing MBTA transit on-time-performance (OTP). "
    "You are given pre-computed, verified findings. Do NOT invent numbers beyond those given. "
    "Be concise, specific (name routes/stops), and practical."
)


class LLMAnalyzer:
    def __init__(self, api_key: str, model: str = "openai/gpt-oss-120b:free",
                 base_url: str = "https://openrouter.ai/api/v1", max_tokens: int = 900):
        self.api_key, self.model, self.base_url, self.max_tokens = api_key, model, base_url, max_tokens

    def _complete(self, system_prompt: str, user: str) -> str:
        from openai import OpenAI
        client = OpenAI(base_url=self.base_url, api_key=self.api_key)
        resp = client.chat.completions.create(
            model=self.model, max_tokens=self.max_tokens,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user}])
        return resp.choices[0].message.content.strip()

    def _prompt(self, real: list[Finding], caveats: list[Finding], system: dict) -> str:
        lines = [f"System OTP: {system.get('system_otp')}% (median {system.get('median_late')} min, "
                 f"avg {system.get('avg_late')} min, {system.get('lateness_rows')} observations).",
                 "Verified findings — kind | subject | detail | known/NEW:"]
        lines += [f"- {f.kind} | {f.subject} | {f.detail} | {'known' if f.is_known else 'NEW'}" for f in real[:40]]
        if caveats:
            lines.append("Data-quality caveats (exclude from conclusions): "
                         + "; ".join(f.detail for f in caveats[:8]))
        lines.append("\nWrite two short sections:\n"
                     "1. **What's notable** (4-6 bullets) — emphasize NEW patterns, the late-vs-early "
                     "split, and corridor/mode clustering.\n"
                     "2. **Proposed new marts/metrics** (up to 3) — concrete and buildable.")
        return "\n".join(lines)

    def narrate(self, real: list[Finding], caveats: list[Finding], system: dict) -> str:
        deterministic = deterministic_narrative(real, caveats, system)
        try:
            prose = self._complete(SYSTEM_PROMPT, self._prompt(real, caveats, system))
        except Exception as e:  # rate-limit / network / bad key → never fail the run
            return f"*(LLM unavailable: {str(e)[:90]} — deterministic narrative only)*\n\n{deterministic}"
        return (f"{prose}\n\n---\n*Narrated by `{self.model}` (read-only proposal). "
                f"Verified findings below are authoritative:*\n\n{deterministic}")
