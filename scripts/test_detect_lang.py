"""Tests for detect_lang() in the two prehraj.to bulk importers.

Issue #537 — hyphen-separated language markers (CS-titulky, cz-dab,
CZ-tit) were classified as UNKNOWN because the connector between the
language tag and the kind tag was plain `\\s*`. After the fix it's
`[\\s\\-_]*`, accepting spaces, hyphens, underscores, or none.

Underscore caveat: Python's `\\b` is a word/non-word transition; an
underscore counts as a word character, so `\\bcz` does NOT match in
`_cz_dab`. Underscore stays in the character class to match the issue
spec but in practice only hyphens and spaces are unblocked. Glued
no-separator forms like `FilmCZdabing` were never matched and remain
so — `\\b` still requires a real word boundary before `cz`.

Existing CZ_NATIVE class is unrelated to the regexes touched here: it
is a diacritic-and-Czech-word heuristic at the tail of detect_lang().
We assert it remains stable.

Run from repo root: python3 scripts/test_detect_lang.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


uploads = _load("_uploads", _HERE / "import-prehrajto-uploads.py")
new_films = _load("_new_films", _HERE / "import-prehrajto-new-films.py")


# Hyphen-separated markers — the gap closed by #537. Each of these
# returned UNKNOWN before the fix and now reaches the matching DUB/SUB
# class.
HYPHEN_CASES: list[tuple[str, str]] = [
    # The four examples called out explicitly in #537.
    ("Na-velikosti-zalezi-Izr-Fr-komedie-2009-CS-titulky-TVrip", "CZ_SUB"),
    ("Iluzionista-_-L'illusionniste-2010,-CZ-tit", "CZ_SUB"),
    # CZ-dab / cz-dab / cs-dabing.
    ("Film-CZ-dab-2020", "CZ_DUB"),
    ("Klan-cz-dabing-rip-2018", "CZ_DUB"),
    ("Film-CS-dabing-2020", "CZ_DUB"),
    # CZ-titulky / cz-tit / cz-subs / CS-titulky.
    ("Film-CZ-titulky-720p", "CZ_SUB"),
    ("Film-cz-tit-rip", "CZ_SUB"),
    ("Film-cz-subs-1080p", "CZ_SUB"),
    ("Film-CS-titulky-rip", "CZ_SUB"),
    # SK hyphen variants.
    ("Film-SK-dab-2020", "SK_DUB"),
    ("Klan-sk-dabing-rip-2018", "SK_DUB"),
    ("Film-SK-titulky-720p", "SK_SUB"),
    ("Film-sk-tit-rip", "SK_SUB"),
]


REGRESSION_CASES: list[tuple[str, str]] = [
    # Space-separated — must keep working.
    ("Film CZ dabing 2020", "CZ_DUB"),
    ("Film CZ titulky 720p", "CZ_SUB"),
    ("Film SK dabing 2020", "SK_DUB"),
    ("Film SK titulky 720p", "SK_SUB"),
    ("Klan cz dab", "CZ_DUB"),
    ("Klan cz tit", "CZ_SUB"),
    # Glued ascii word-prefix variants that *do* match (\\bcz still
    # needs a real word boundary, so these rely on a separator before
    # the cz prefix).
    ("Film cz.dab 2020", "CZ_DUB"),
    ("Film czdab rip", "CZ_DUB"),
    ("Film cztit rip", "CZ_SUB"),
    # Czech-word variants that match via the ascii-only `cesk[aáyý]`
    # / `cesk[yé]` branches.
    ("Film ceska dabing 2024", "CZ_DUB"),
    ("Film cesky dabing", "CZ_DUB"),
    ("Film cesky titulky 2024", "CZ_SUB"),
    # Slovak ascii variants.
    ("Film slovensky dabing", "SK_DUB"),
    ("Film slovenina dab", "SK_DUB"),
    # Existing CZ_NATIVE heuristic (diacritics + Czech word) must not
    # be flipped by the regex broadening — these have a Czech particle
    # ("v", "na", etc.) plus diacritics and intentionally no DUB/SUB
    # keyword. Still CZ_NATIVE after the fix.
    ("Hořkosladký film na hranicích", "CZ_NATIVE"),
    ("Skvělá česká komedie v originále", "CZ_NATIVE"),
    # Negative — no language marker, must NOT be DUB/SUB. Whether the
    # diacritic heuristic picks them up as CZ_NATIVE is separate.
    ("Mission Impossible 2020 1080p", "_NOT_DUBSUB"),
    ("Yellowstone S05E01 1080p", "_NOT_DUBSUB"),
    # Standalone bare-tag titles. After #537 these stay UNKNOWN —
    # promotion of bare `-CZ-` / `-CS-` to CZ_NATIVE is tracked
    # separately in #714, not in this PR.
    ("Tah-jezdcem-CZ-1992-(DANiELS)", "UNKNOWN"),
    # Under_score connector — included in the regex character class
    # per spec but blocked by Python's \\b. Document the limitation:
    # this stays UNKNOWN even after #537.
    ("Movie_cz_dabing", "UNKNOWN"),
    # Glued no-separator before the cz prefix — \\b cannot fire, so
    # this remains UNKNOWN. (`FilmCZdabing`-style.)
    ("FilmCZdabing2020", "UNKNOWN"),
]


def _check(mod, title: str, expected: str) -> str | None:
    got = mod.detect_lang(title)
    if expected == "_NOT_DUBSUB":
        if got in ("CZ_DUB", "CZ_SUB", "SK_DUB", "SK_SUB"):
            return f"got {got!r}, expected not-in-DUB/SUB"
        return None
    if got != expected:
        return f"got {got!r}, expected {expected!r}"
    return None


def main() -> int:
    failures: list[str] = []

    for module_name, mod in [("uploads.py", uploads),
                             ("new-films.py", new_films)]:
        for title, expected in HYPHEN_CASES + REGRESSION_CASES:
            err = _check(mod, title, expected)
            if err:
                failures.append(f"[{module_name}] {title!r}: {err}")

    # Cross-file consistency: the regex block is duplicated 1:1 in the
    # two scripts. If they drift, the safer of the two will mask the
    # bug on the other in production. Catch it here.
    for attr in ("CZ_DUB_RE", "CZ_SUB_RE", "SK_DUB_RE", "SK_SUB_RE"):
        u = getattr(uploads, attr).pattern
        n = getattr(new_films, attr).pattern
        if u != n:
            failures.append(f"{attr} drift: uploads={u!r} vs new-films={n!r}")

    if failures:
        print(f"FAIL ({len(failures)})", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    total = (len(HYPHEN_CASES) + len(REGRESSION_CASES)) * 2 + 4
    print(f"OK — {total} assertions passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
