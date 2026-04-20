"""Thin wrapper around Gemma 4 (Gemini API) for unique CS description generation.

Reuses the prompt template and call logic from `generate-film-descriptions.py`
but exposes a single `generate_unique_cs(...)` function suitable for the
auto-import pipeline. Handles safety-filter refusals gracefully by returning
None — caller falls back to TMDB CS overview as-is.

Env vars (priority order):
    GEMINI_API_KEY      — single key (preferred for production cron)
    GEMINI_API_KEY_1..4 — parallel dev keys (reused if GEMINI_API_KEY missing)
"""

from __future__ import annotations

import logging
import os
import time

import requests

MODEL = "gemma-3-27b-it"
URL_TPL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={{}}"
DEFAULT_TIMEOUT = 120
RATE_LIMIT_PAUSE = 60

log = logging.getLogger(__name__)


def _load_keys() -> list[str]:
    single = os.environ.get("GEMINI_API_KEY", "").strip()
    if single:
        return [single]
    out = []
    for i in range(1, 5):
        k = os.environ.get(f"GEMINI_API_KEY_{i}", "").strip()
        if k:
            out.append(k)
    return out


def _build_prompt_film(title: str, year: int | None, sources: list[tuple[str, str]]) -> str:
    parts = [f"Tady jsou popisy filmu {title} ({year or '?'}) z různých zdrojů:\n"]
    for i, (name, text) in enumerate(sources, 1):
        parts.append(f"Zdroj {i} ({name}):\n{text}\n")
    parts.append(
        "Na základě výše uvedených popisů napiš JEDEN krátký originální český "
        "popis tohoto filmu.\nPožadavky: 3-6 vět, 150-400 znaků, poutavý styl, "
        "vlastní formulace (ne kopie ze zdrojů).\nPiš přímo o ději a postavách. "
        "Nekomentuj zadání, nepiš odrážky, nepiš nadpis.\nPiš POUZE česky — i "
        "když jsou zdroje anglicky, výstup musí být v plynulé češtině.\n"
        "Odpověz pouze samotným textem popisu:"
    )
    return "\n".join(parts)


def _build_prompt_series(title: str, year: int | None, sources: list[tuple[str, str]]) -> str:
    parts = [f"Tady jsou popisy seriálu {title} ({year or '?'}) z různých zdrojů "
             "(některé česky, některé anglicky):\n"]
    for i, (name, text) in enumerate(sources, 1):
        parts.append(f"Zdroj {i} ({name}):\n{text}\n")
    parts.append(
        "Na základě výše uvedených popisů napiš JEDEN originální český popis "
        "tohoto seriálu.\nPožadavky: 4-7 vět, 300-600 znaků, poutavý styl, "
        "vlastní formulace.\nPiš o příběhu, hlavních postavách a atmosféře "
        "seriálu. Výstup musí být POUZE česky.\nNekomentuj zadání, nepiš "
        "odrážky, nepiš nadpis. Odpověz pouze samotným textem popisu:"
    )
    return "\n".join(parts)


def _call(prompt: str, key: str, timeout: int = DEFAULT_TIMEOUT) -> str | None:
    """Single Gemini call. Returns generated text or None on safety-filter / error."""
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 1000},
    }
    try:
        r = requests.post(URL_TPL.format(key), json=payload, timeout=timeout)
    except requests.RequestException as e:
        log.warning("Gemini call failed: %s", e)
        return None
    if r.status_code == 429:
        log.warning("Gemini rate-limited; sleeping %ds", RATE_LIMIT_PAUSE)
        time.sleep(RATE_LIMIT_PAUSE)
        return None
    if r.status_code != 200:
        log.warning("Gemini HTTP %d: %s", r.status_code, r.text[:200])
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    cands = data.get("candidates") or []
    if not cands:
        # Most common reason: safety filter blocked the response
        log.info("Gemini returned no candidates (safety filter?)")
        return None
    parts = cands[0].get("content", {}).get("parts") or []
    if not parts:
        return None
    text = (parts[0].get("text") or "").strip()
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    return text or None


def generate_unique_cs(
    title: str,
    year: int | None,
    sources: list[tuple[str, str]],
    *,
    is_series: bool = False,
) -> str | None:
    """Generate a unique Czech description from one or more source texts.

    Args:
        title: film/series name (used in the prompt for context)
        year: optional year
        sources: list of (source_name, text) tuples — at least 1 required
        is_series: True for TV series, picks longer-text prompt template

    Returns:
        Generated CS text, or None if generation failed (safety filter,
        rate limit, network error, missing API key, no source texts).
    """
    if not sources:
        return None
    keys = _load_keys()
    if not keys:
        log.warning("No GEMINI_API_KEY env var set — Gemma generation disabled")
        return None
    builder = _build_prompt_series if is_series else _build_prompt_film
    prompt = builder(title, year, sources)
    return _call(prompt, keys[0])


# ---------------------------------------------------------------------------
# Public API for external batch jobs
# ---------------------------------------------------------------------------
# Batch scripts that drive their own key rotation + retry policy (e.g.
# scripts/generate-film-descriptions-prehrajto.py) need direct access to
# the prompt builder, the key loader, and a one-shot Gemini call. Expose
# stable non-underscore aliases so those callers don't depend on what the
# internal API happens to be named today.

build_prompt_film = _build_prompt_film
build_prompt_series = _build_prompt_series
load_keys = _load_keys


def call_gemma(prompt: str, key: str, timeout: int = DEFAULT_TIMEOUT) -> str | None:
    """Single Gemini call with a caller-supplied key.

    Returns the generated text, or None on:
      * HTTP 429 (after sleeping `RATE_LIMIT_PAUSE` seconds)
      * safety-filter refusal (no candidates returned)
      * HTTP != 200
      * network / JSON errors

    The caller decides whether to retry — this function does not loop."""
    return _call(prompt, key, timeout)
