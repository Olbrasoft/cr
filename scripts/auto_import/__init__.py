"""Auto-import pipeline for SK Torrent (Issue #413).

Modules in this PR:
    sktorrent_scanner — listing crawler with checkpoint-based stop

Planned in separate sub-issues of #413:
    title_parser      — structured data from SK Torrent title strings (#416)
    sktorrent_detail  — fetch detail page, extract cdn + qualities (#417)
    tmdb_resolver     — resolve IMDB ID via TMDB search (#418)
    enricher          — orchestrate add/update of films/series/episodes (#419, #420)
"""
