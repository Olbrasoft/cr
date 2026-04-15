"""Smoke tests for title_parser. Run: python3 -m scripts.auto_import.test_title_parser"""

from scripts.auto_import.title_parser import parse_sktorrent_title


CASES = [
    # (input, expected subset of fields to check)
    ("Pomocnice / The Housemaid (2025)(CZ)",
     {"cz_title": "Pomocnice", "en_title": "The Housemaid", "year": 2025,
      "is_episode": False, "langs": ["CZ"]}),
    ("Euforie / Euphoria / S03E01 / CZ",
     {"cz_title": "Euforie", "en_title": "Euphoria", "season": 3, "episode": 1,
      "is_episode": True, "langs": ["CZ"]}),
    ("Hitler: Vzestup zla / Hitler: The Rise of Evil (2003)(720p)(CZ)  = CSFD 82%",
     {"cz_title": "Hitler: Vzestup zla", "en_title": "Hitler: The Rise of Evil",
      "year": 2003, "quality": "720p", "csfd_rating": 82}),
    ("Ninjova nadvláda / Ninja III: The Domination (1984)(CZ)",
     {"cz_title": "Ninjova nadvláda", "en_title": "Ninja III: The Domination",
      "year": 1984, "is_episode": False}),
    ("Mayové a jejich poslední velká města (2024)",
     {"cz_title": "Mayové a jejich poslední velká města", "en_title": None,
      "year": 2024, "is_episode": False}),
    ("Skryté skvosty-Hrádek u Nechanic (S01E01)",
     {"season": 1, "episode": 1, "is_episode": True}),
    ("Teorie velkého třesku S01E01-17 2007 CZ dab 1080p",
     {"season": 1, "episode": 1, "year": 2007, "quality": "1080p",
      "is_episode": True, "langs": ["DUB_CZ"]}),
    ("",
     {"cz_title": None, "en_title": None, "is_episode": False}),
]


def main() -> int:
    fail = 0
    for raw, expected in CASES:
        got = parse_sktorrent_title(raw).to_dict()
        problems = []
        for k, v in expected.items():
            actual = got.get(k)
            if actual != v:
                problems.append(f"  {k}: expected {v!r}, got {actual!r}")
        marker = "OK " if not problems else "FAIL"
        print(f"[{marker}] {raw[:60]!r}")
        if problems:
            fail += 1
            for p in problems:
                print(p)
    print(f"\n{len(CASES) - fail}/{len(CASES)} OK")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
