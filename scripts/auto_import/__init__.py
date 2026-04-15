"""Auto-import pipeline for SK Torrent (Issue #413).

Modules:
    sktorrent_scanner — listing crawler with checkpoint-based stop
    title_parser      — extract structured data from SK Torrent title strings
    sktorrent_detail  — fetch detail page, extract cdn + qualities
    tmdb_resolver     — resolve IMDB ID via TMDB search
    enricher          — orchestrate add/update of films/series/episodes
"""
