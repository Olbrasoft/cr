"""Smoke tests for tmdb_resolver helpers.

Run: python3 -m scripts.auto_import.test_tmdb_resolver

Currently covers only `is_usable_cs_title` — the detector that rejects TMDB's
cs-CZ fallback when it returns the original-language title instead of a real
Czech translation (see issue #572).
"""

from scripts.auto_import.tmdb_resolver import is_usable_cs_title


# (label, cs_title, en_title, original_title, expected_usable)
CASES = [
    # Genuine Czech translations — keep.
    ("czech-translation", "Pán prstenů", "The Lord of the Rings", "The Lord of the Rings", True),
    ("czech-with-diacritics", "Žížaly útočí", "Worms Attack", "Worms Attack", True),
    ("czech-all-ascii", "Matrix", "The Matrix", "The Matrix", True),

    # TMDB fallback echoed original-language title — drop.
    ("echoed-original-hindi", "दिल बेचारा", "Dil Bechara", "दिल बेचारा", False),
    ("echoed-original-japanese", "アルキメデスの大戦", "The Great War of Archimedes", "アルキメデスの大戦", False),

    # cs_title == en_title (no separate Czech translation) — drop.
    ("cs-equals-en", "The Matrix", "The Matrix", "The Matrix", False),

    # cs_title contains non-Latin glyphs even if different from original_title — drop.
    ("korean-hangul", "검은 사제들", "The Priests", "검은 사제들", False),
    ("chinese-han", "攀登者", "The Climbers", "攀登者", False),
    ("cyrillic-russian", "Мой ласковый и нежный зверь", "My Tender and Affectionate Beast",
     "Мой ласковый и нежный зверь", False),
    ("thai", "รักแห่งสยาม", "The Love of Siam", "รักแห่งสยาม", False),
    ("hebrew", "סיפורי בית קפה", "Cafe Tales", "סיפורי בית קפה", False),
    ("arabic", "باب الحديد", "Cairo Station", "باب الحديد", False),
    ("devanagari-mixed-with-ascii", "Commando 2 - दबंग्ग", "Commando 2", "Commando 2", False),

    # Edge cases.
    ("empty", "", "The Matrix", "The Matrix", False),
    ("none-cs", None, "The Matrix", "The Matrix", False),

    # Czech title that happens to share Latin characters with original — keep.
    ("czech-short-with-ascii", "Ano", "Yes", "Yes", True),
]


def main() -> int:
    fail = 0
    for label, cs_title, en_title, original_title, expected in CASES:
        got = is_usable_cs_title(cs_title, en_title, original_title)
        marker = "OK " if got == expected else "FAIL"
        if got != expected:
            fail += 1
        print(f"[{marker}] {label}: got={got} expected={expected}")
    print(f"\n{len(CASES) - fail}/{len(CASES)} OK")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
