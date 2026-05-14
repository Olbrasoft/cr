"""Parse SK Torrent video titles into structured data.

SK Torrent titles follow a loose convention:
    "<CZ_NAME> / <EN_NAME>[ / SxxExx][ (YEAR)][ (QUALITY)][ (LANG)][ = CSFD N%]"

Examples:
    "Pomocnice / The Housemaid (2025)(CZ)"
        → {cz: "Pomocnice", en: "The Housemaid", year: 2025, langs: [CZ]}
    "Euforie / Euphoria / S03E01 / CZ"
        → {cz: "Euforie", en: "Euphoria", season: 3, episode: 1, langs: [CZ]}
    "Hitler: Vzestup zla / Hitler: The Rise of Evil (2003)(720p)(CZ) = CSFD 82%"
        → {cz: "...", en: "...", year: 2003, quality: "720p", langs: [CZ], csfd_rating: 82}
    "Ninja Resurrection / Ninpuu kamui gaiden / OVA 1/2 / jap. s cz. tit"
        → {cz: "Ninja Resurrection", en: "Ninpuu kamui gaiden", langs: [SUBS_CZ]}

Parser is intentionally lenient: unknown fields stay None, no exception is
raised on malformed input. Caller (TMDB resolver) is responsible for treating
missing fields as a softer match signal.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, asdict, field


# Episode marker like S03E01, s3e1, 03x01, 3x1
_EPISODE_RE = re.compile(r"\bS(\d{1,2})E(\d{1,2})\b", re.IGNORECASE)
_EPISODE_X_RE = re.compile(r"\b(\d{1,2})x(\d{1,2})\b")

# SK Torrent's CZ-series convention: `Title / E32 / Subtitle / CZ`. No
# season number — uploads use a single continuous episode count across
# the whole series (Pat a Mat, Krtek, etc.). We treat these as season 1
# so `is_episode=True` and the auto-import routes to the series
# enricher instead of fabricating a film row from a spurious TMDB
# match. Slash-bounded on BOTH sides so a stray "E32" inside a title
# like "Final Frontier E32" doesn't accidentally match.
_SLASH_EPISODE_RE = re.compile(r"/\s*E(\d{1,3})\s*/", re.IGNORECASE)

# Year in parentheses or anywhere as standalone 4-digit
_YEAR_PAREN_RE = re.compile(r"\((19|20)\d{2}\)")
_YEAR_BARE_RE = re.compile(r"\b(19|20)\d{2}\b")

# Quality markers (in parens or bracketed)
_QUALITY_RE = re.compile(r"\b(2160p|1080p|1080i|720p|480p|HD|UHD|4K)\b", re.IGNORECASE)

# CSFD rating "= CSFD 82%" or "(CSFD 82%)"
_CSFD_RE = re.compile(r"CSFD\s*(\d{1,3})\s*%", re.IGNORECASE)

# Czech / Slovak dub markers (used by _detect_langs)
_DUB_CZ_RE = re.compile(
    r"\b(cz[\s\-]?dab|cz[\s\-]?dabing|cz[\s\-]?dub|"
    r"cze?s\.?\s*dab|český\s*dabing?)\b",
    re.IGNORECASE,
)
_DUB_SK_RE = re.compile(
    r"\b(sk[\s\-]?dab|slovenský\s*dabing?)\b",
    re.IGNORECASE,
)
_SUBS_RE = re.compile(
    r"\b(cz[\s\-]?tit(?:ulky)?|cztit|sk[\s\-]?tit(?:ulky)?|"
    r"czech\s*subs?|cz\s*subs?|cz\.?\s*tit\.?|s\s*cz\.?\s*tit\.?)\b",
    re.IGNORECASE,
)
_SUBS_SK_RE = re.compile(r"\bsk[\s\-]?tit", re.IGNORECASE)
# "(CZ)" or " / CZ" suffix (uppercase, surrounded by separators).
# CZE is normalized to CZ; EN is allowed as a documented value.
_LANG_TAG_RE = re.compile(r"(?:^|[\(\[/])\s*(CZ|SK|EN|CZE)\s*(?:[\)\]/]|$)", re.IGNORECASE)

# Strip noise like (1080p), [TvRip], (CZ Dabing)
_STRIP_PARENS_RE = re.compile(r"\s*[\(\[][^\)\]]*[\)\]]")


@dataclass
class ParsedTitle:
    cz_title: str | None = None
    en_title: str | None = None
    year: int | None = None
    season: int | None = None
    episode: int | None = None
    quality: str | None = None       # "1080p", "720p", "480p"
    # Subset of ["DUB_CZ","DUB_SK","SUBS_CZ","SUBS_SK","CZ","SK","EN"].
    # CZE in source titles is normalized to "CZ".
    langs: list[str] = field(default_factory=list)
    csfd_rating: int | None = None
    is_episode: bool = False         # True if SxxExx detected
    raw: str = ""                    # original input

    def to_dict(self) -> dict:
        return asdict(self)


def _detect_episode(title: str) -> tuple[int | None, int | None]:
    """Return (season, episode) if title contains SxxExx or NxM marker."""
    m = _EPISODE_RE.search(title)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _EPISODE_X_RE.search(title)
    if m:
        return int(m.group(1)), int(m.group(2))
    # SK Torrent's `/ E32 /` shorthand — no season annotation. Default
    # to season 1 so downstream code can treat it as an episode of an
    # un-seasoned series.
    m = _SLASH_EPISODE_RE.search(title)
    if m:
        return 1, int(m.group(1))
    return None, None


def _detect_year(title: str) -> int | None:
    """Year in parentheses wins over a bare 4-digit year (less ambiguous)."""
    m = _YEAR_PAREN_RE.search(title)
    if m:
        return int(m.group(0)[1:5])
    # Bare year — pick the LAST one (titles like "Top Gun 2 2022" should give 2022)
    matches = list(_YEAR_BARE_RE.finditer(title))
    if matches:
        return int(matches[-1].group(0))
    return None


def _detect_quality(title: str) -> str | None:
    m = _QUALITY_RE.search(title)
    if not m:
        return None
    raw = m.group(1).lower()
    # Normalize "HD"/"4K"/"UHD" to a resolution number where possible
    if raw == "uhd" or raw == "4k":
        return "2160p"
    if raw == "hd":
        return "720p"  # SK Torrent's "HD" badge means ≥ 720p; conservative default
    return raw


def _detect_csfd(title: str) -> int | None:
    m = _CSFD_RE.search(title)
    return int(m.group(1)) if m else None


def _detect_langs(title: str) -> list[str]:
    """Best-effort language flags. Order: dubs first, then subs."""
    flags = []
    if _DUB_CZ_RE.search(title):
        flags.append("DUB_CZ")
    if _DUB_SK_RE.search(title):
        flags.append("DUB_SK")
    if _SUBS_RE.search(title):
        flags.append("SUBS_SK" if _SUBS_SK_RE.search(title) else "SUBS_CZ")
    if not flags:
        # Fallback to bare CZ/SK/EN tag (e.g. "Euphoria / S03E01 / CZ" — bare CZ context only)
        m = _LANG_TAG_RE.search(title)
        if m:
            tag = m.group(1).upper()
            if tag == "CZE":
                tag = "CZ"  # normalize ISO-639-2 form
            flags.append(tag)
    return flags


def _split_titles(title: str) -> tuple[str | None, str | None]:
    """Split the title on `/` separators and return (cz, en).

    Heuristic:
      1. Drop trailing segments that are obviously NOT titles:
         year-only, language-only, episode-only (S##E##), quality-only.
      2. The first remaining segment = cz_title, second = en_title (if any).
    """
    parts = [p.strip() for p in title.split("/")]
    parts = [p for p in parts if p]

    def is_noise(s: str) -> bool:
        s_clean = s.strip()
        # Strip parens content first
        s_naked = _STRIP_PARENS_RE.sub("", s_clean).strip()
        if not s_naked:
            return True
        # Episode-only segment
        if _EPISODE_RE.fullmatch(s_naked) or _EPISODE_X_RE.fullmatch(s_naked):
            return True
        # Year-only — but preserve a sole segment so legitimate 4-digit
        # titles like "1917 (2019)(CZ)" aren't discarded entirely.
        if _YEAR_BARE_RE.fullmatch(s_naked) and len(parts) > 1:
            return True
        # Language tag only
        if re.fullmatch(r"(CZ|SK|EN|cz\s*dabing?|sk\s*dabing?|cz\s*tit(?:ulky)?|"
                        r"jap\.?\s*s\s*cz\.?\s*tit\.?)", s_naked, re.IGNORECASE):
            return True
        # Quality only
        if _QUALITY_RE.fullmatch(s_naked):
            return True
        return False

    title_parts = [p for p in parts if not is_noise(p)]

    def clean(p: str) -> str:
        # Strip trailing parens/brackets (year, quality, lang annotations)
        s = _STRIP_PARENS_RE.sub("", p).strip()
        # Strip trailing CSFD rating
        s = _CSFD_RE.sub("", s).strip()
        # Strip SxxExx (and everything after it). SK Torrent episode titles
        # without a `/` separator between show and marker look like
        # "Královny Brna S01E03" or "Výměna manželek S03E01 - Katka a Denisa";
        # keep only the show name. Also handles "NxM" form.
        for rx in (_EPISODE_RE, _EPISODE_X_RE):
            m = rx.search(s)
            if m:
                s = s[: m.start()].rstrip()
        # Strip dangling = signs / dashes / dots
        s = re.sub(r"\s*[=\-•·.]+\s*$", "", s).strip()
        return s

    cz = clean(title_parts[0]) if len(title_parts) >= 1 else None
    en = clean(title_parts[1]) if len(title_parts) >= 2 else None
    return cz or None, en or None


def parse_sktorrent_title(title: str) -> ParsedTitle:
    """Parse a SK Torrent title string. Never raises.

    Returns the structured ParsedTitle dataclass; callers that prefer a plain
    dict can use `parse_sktorrent_title(...).to_dict()`.
    """
    if not title:
        return ParsedTitle(raw="")
    # Decode HTML entities — SK Torrent occasionally double-escapes `&` in the
    # listing's `title="..."` attribute (e.g. "Survivor Česko &amp;amp;
    # Slovensko"). One unescape pass handles the normal case; the second pass
    # catches double-encoded strings. `html.unescape` is a no-op on clean text.
    raw = html.unescape(html.unescape(title)).strip()
    season, episode = _detect_episode(raw)
    cz, en = _split_titles(raw)
    return ParsedTitle(
        cz_title=cz,
        en_title=en,
        year=_detect_year(raw),
        season=season,
        episode=episode,
        quality=_detect_quality(raw),
        langs=_detect_langs(raw),
        csfd_rating=_detect_csfd(raw),
        is_episode=season is not None and episode is not None,
        raw=raw,
    )


# Bracketed noise blocks ([...] or (...)) — prehraj.to uploaders pack release
# metadata (resolution, codec, lang) into brackets after the show name. Strip
# them before SxxExx-trim so the leftover show title is clean.
_BRACKET_NOISE_RE = re.compile(r"[\[\(][^\]\)]*[\]\)]")

# Release-tag scaffolding that survives bracket stripping (e.g. "S01E03 1080p
# WEBRip x264 CZ-DAB" — uploaders skip brackets and just append flags with
# spaces / dots / dashes). Match each token in isolation so we don't eat real
# words from a title that happens to contain them.
_PRH_TAIL_NOISE_RE = re.compile(
    r"\b(?:"
    r"2160p|1080p|1080i|720p|480p|360p|"
    r"4k|uhd|fhd|hd|"
    r"bdrip|bluray|blu[\s\-]?ray|webrip|web[\s\-]?dl|hdrip|dvdrip|hdtv|tvrip|cam|ts|"
    r"x264|x265|h\.?264|h\.?265|hevc|xvid|divx|"
    r"aac|ac3|dts|mp3|5\.1|7\.1|"
    r"avi|mkv|mp4|"
    r"cz[\s\-\.]?dab(?:ing)?|cz[\s\-\.]?dub|cz[\s\-\.]?tit(?:ulky)?|"
    r"sk[\s\-\.]?dab(?:ing)?|sk[\s\-\.]?dub|sk[\s\-\.]?tit(?:ulky)?|"
    r"cesk[yáé]\s*dab(?:ing)?|cesk[yé]\s*titulky|cesky\s*dabing|"
    r"slovensk[yáé]\s*dab(?:ing)?|"
    r"czdab|cztit|skdab|sktit|engsub|engdub|eng[\s\-]?sub|eng[\s\-]?only|"
    r"cz|sk|en|cze?s|"
    r"cely\s*film|cely|remastered|extended|uncut|directors?\s*cut|"
    r"novinka|premiera|hit"
    r")\b",
    re.IGNORECASE,
)

# "- Episode/Epizoda N" / "- Ep N" suffix uploaders sometimes spell out
# instead of using SxxExx (e.g. "Yellowstone - Episode 3"). Strip BEFORE
# tail-noise so the digit doesn't survive as a bare token.
_PRH_SPELLED_EPISODE_RE = re.compile(
    r"\s*-\s*(?:Episode|Epizoda|Epizode|Episód[ay]|Epis|Ep)\.?\s+\d{1,3}\s*$",
    re.IGNORECASE,
)


def _strip_prehrajto_noise(s: str) -> str:
    """Truncate a prehraj.to upload title at the FIRST recognised metadata
    boundary (SxxExx, NxM, "- Episode N", year-in-parens), returning the
    bare show name.

    Why truncate, not strip: prehraj.to uploaders glue the episode name
    AFTER the episode marker, e.g.
        "The Last of Us S01E02-Nakažení-2023 CZ-DAB"
        "Yellowstone S05E01 - Half the Money"
    Strip-then-collapse would leave "The Last of Us Nakažení", which
    won't normalize-match the series alias "thelastofus" — the alias
    filter would then reject every multi-episode show that names its
    episodes in the upload title (i.e. most CS/SK fan-uploads).

    Truncating at the first marker mirrors what `parse_sktorrent_title`
    already does for SK Torrent titles (see `_split_titles.clean`
    line ~192) — same convention across both providers.

    Steps:
      1. peel brackets / parens — they're always release metadata, not
         part of the show name (e.g. "(2019)", "[CZ-DAB]")
      2. find the earliest boundary among {SxxExx, NxM, "- Episode N",
         standalone year} and truncate there
      3. strip leftover release tokens that survive (uploads with no
         marker at all, e.g. "Yellowstone 1080p WEBRip CZ-DAB" — rare
         since the caller will reject these via `is_episode=False`,
         but still desirable for consistent normalization)
      4. collapse whitespace + trim
    """
    s = _BRACKET_NOISE_RE.sub(" ", s)
    s = _PRH_SPELLED_EPISODE_RE.sub("", s)

    # Earliest match wins — pick the lowest start position across the
    # boundary patterns and truncate there.
    boundaries: list[int] = []
    for rx in (_EPISODE_RE, _EPISODE_X_RE, _YEAR_BARE_RE):
        m = rx.search(s)
        if m:
            boundaries.append(m.start())
    if boundaries:
        s = s[: min(boundaries)]

    s = _PRH_TAIL_NOISE_RE.sub(" ", s)
    s = re.sub(r"\s*[=\-•·.,/_]+\s*", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_prehrajto_episode_title(title: str) -> ParsedTitle:
    """Parse a prehraj.to upload title — never raises.

    Prehraj.to uploaders don't use `/` as a CZ/EN separator (the way SK
    Torrent does), so `_split_titles` would mis-parse the title. Instead
    we strip the metadata noise directly and emit the bare show name as
    `cz_title` (it's whichever language the uploader chose — CS or EN —
    so the caller has to alias-match against BOTH `series.title` and
    `series.original_title` to be safe).

    Examples:
      "Yellowstone S05E01 1080p WEBRip CZ-DAB" →
          cz_title="Yellowstone", season=5, episode=1, year=None
      "Eufórie S02E03 [CZ-DAB] (2019)" →
          cz_title="Eufórie", season=2, episode=3, year=2019
      "The Last of Us 1x05 [1080p]" →
          cz_title="The Last of Us", season=1, episode=5

    `is_episode` is True only if BOTH season + episode were detected,
    matching the existing convention in `parse_sktorrent_title`.
    """
    if not title:
        return ParsedTitle(raw="")
    raw = html.unescape(html.unescape(title)).strip()
    season, episode = _detect_episode(raw)
    year = _detect_year(raw)
    quality = _detect_quality(raw)
    langs = _detect_langs(raw)
    clean = _strip_prehrajto_noise(raw)
    return ParsedTitle(
        cz_title=clean or None,
        en_title=None,
        year=year,
        season=season,
        episode=episode,
        quality=quality,
        langs=langs,
        csfd_rating=_detect_csfd(raw),
        is_episode=season is not None and episode is not None,
        raw=raw,
    )
