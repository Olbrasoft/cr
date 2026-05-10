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

URL_TPL = ("https://generativelanguage.googleapis.com/v1beta/models/"
           "{model}:generateContent?key={key}")
DEFAULT_TIMEOUT = 120
RATE_LIMIT_PAUSE = 60

# Try models in this order. Google's `gemma-4-31b-it` started returning
# HTTP 500 INTERNAL on every prompt around 2026-05-10 (verified independently
# with all four production keys + a one-word "Řekni ahoj" payload). The
# 26B-A4B variant works fine and produces equivalent quality CS output, so
# it's now primary; we keep 31B in the fallback chain so the moment Google
# fixes the outage we transparently regain access to it (and so a future
# regression in 26B doesn't kill the import path either). The auto-import
# silently fell back to raw TMDB EN overview for every single film during
# the outage — that's the bug this list prevents.
MODELS = ["gemma-4-26b-a4b-it", "gemma-4-31b-it"]
MODEL = MODELS[0]  # backwards-compat for callers that import the symbol

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


def _call(prompt: str, key: str, timeout: int = DEFAULT_TIMEOUT,
          model: str = MODEL) -> str | None:
    """Single Gemini call. Returns generated text or None on safety-filter / error."""
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        # Gemma 4 emits hidden reasoning tokens before the answer, eating
        # the output budget. Bump high enough that the answer survives.
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 3000},
    }
    try:
        r = requests.post(
            URL_TPL.format(model=model, key=key), json=payload, timeout=timeout
        )
    except requests.RequestException as e:
        log.warning("Gemini call failed (%s): %s", model, e)
        return None
    if r.status_code == 429:
        log.warning("Gemini rate-limited (%s); sleeping %ds", model, RATE_LIMIT_PAUSE)
        time.sleep(RATE_LIMIT_PAUSE)
        return None
    if r.status_code != 200:
        log.warning("Gemini HTTP %d (%s): %s", r.status_code, model, r.text[:200])
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
    # Gemma 4 returns reasoning in parts[*] with "thought": true and the
    # actual answer in a separate part without that flag. Skip thought
    # parts and join the rest. Older Gemma 3 models don't set the flag,
    # so this still works for them.
    answer_parts = [p.get("text", "") for p in parts if not p.get("thought")]
    text = "\n".join(t for t in answer_parts if t).strip()
    if not text:
        text = (parts[-1].get("text") or "").strip()
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
        log.warning("No GEMINI_API_KEY (or GEMINI_API_KEY_1..4) env var set "
                    "— Gemma generation disabled")
        return None
    builder = _build_prompt_series if is_series else _build_prompt_film
    prompt = builder(title, year, sources)
    # Try each model in MODELS until one returns text. A 5xx or empty
    # response on the primary used to silently fall back to raw TMDB EN
    # — now we transparently retry with the next model first.
    for model in MODELS:
        text = _call(prompt, keys[0], model=model)
        if text:
            return text
    return None


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


def call_gemma(prompt: str, key: str, timeout: int = DEFAULT_TIMEOUT,
               model: str | None = None) -> str | None:
    """Single Gemini call with a caller-supplied key.

    Returns the generated text, or None on:
      * HTTP 429 (after sleeping `RATE_LIMIT_PAUSE` seconds)
      * safety-filter refusal (no candidates returned)
      * HTTP != 200
      * network / JSON errors

    The caller decides whether to retry — this function does not loop.
    `model` defaults to the primary entry of `MODELS`."""
    return _call(prompt, key, timeout, model=model or MODEL)
