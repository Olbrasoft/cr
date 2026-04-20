-- Per-upload metadata for every prehraj.to video linked to a film. One film
-- can have many uploads (observed max in pilot: 9× the same film) with
-- different language markers and resolutions. Import source is the prehraj.to
-- /sitemap/index.xml catalog; details are filled from the sitemap itself (no
-- extra page scrape).
--
-- Serves three consumers:
--   1) Detail-page "Další zdroje" (dnes live scrape) — čte z téhle tabulky.
--   2) /api/movies/stream/<upload_id> player endpoint — vybírá primární
--      upload a má deterministický fallback, když upload na prehraj.to zmizí.
--   3) Audio filter na /filmy-a-serialy — agregované booleany na `films`
--      (viz níž) se plní jako UNION přes alive uploady.
--
-- Existující sloupce `films.prehrajto_has_dub` a `films.prehrajto_has_subs`
-- z migrace 20260423_032 zůstávají. Jejich sémantika se po importu
-- rozšíří — `has_dub` = "má CZ audio" (zahrnuje CZ_DUB i CZ_NATIVE,
-- uživatel v UI chce prostě "zní česky"). `has_subs` = "má CZ titulky".

CREATE TABLE IF NOT EXISTS film_prehrajto_uploads (
    film_id          INTEGER     NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    -- 13-hex ID z konce detail URL (prehraj.to/<slug>/<13-hex>). Stabilní
    -- identifikátor uploadu; první 8 hex = unix timestamp vytvoření.
    upload_id        TEXT        NOT NULL,
    -- Kanonická detail URL. Permanentní, token se scrapuje z ní lazy.
    url              TEXT        NOT NULL,
    -- Původní titulek z uploadera (obsahuje jazykové markery, rozlišení,
    -- ripovací tag atd.). Slouží jako vstup pro detekci jazyka.
    title            TEXT        NOT NULL,
    duration_sec     INTEGER,
    view_count       INTEGER,
    -- Detekovaná jazyková třída z titulku. Viz report.py::detect_lang
    -- v pilotu pro přesný regex.
    lang_class       TEXT        NOT NULL DEFAULT 'UNKNOWN',
    -- Rozlišení z titulku (1080p, 720p, DVDRip, …). Parsing je heuristika,
    -- proto TEXT bez constraintu.
    resolution_hint  TEXT,
    discovered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- FALSE znamená "sitemap ho už několik běhů neviděl" nebo "scrape detailu
    -- vrátil 404 / no contentUrl". Primární výběr je přeskakuje.
    is_alive         BOOLEAN     NOT NULL DEFAULT TRUE,

    PRIMARY KEY (film_id, upload_id),
    CONSTRAINT film_prehrajto_uploads_lang_check CHECK (
        lang_class IN ('CZ_DUB', 'CZ_NATIVE', 'CZ_SUB',
                       'SK_DUB', 'SK_SUB', 'EN', 'UNKNOWN')
    )
);

-- Hlavní čtecí vzor: "všechny alive uploady pro daný film, seřazené" (Další
-- zdroje + výběr primárního). Film_id + is_alive pokrývá WHERE klauzuli.
CREATE INDEX IF NOT EXISTS idx_fpu_film_alive
    ON film_prehrajto_uploads (film_id)
    WHERE is_alive;

-- Sekundární: reconciliation job hledá uploady, které se dlouho neviděly
-- (WHERE last_seen_at < NOW() - INTERVAL '30 days'), typicky přes celou
-- tabulku — index na last_seen_at pomáhá této periodické údržbě.
CREATE INDEX IF NOT EXISTS idx_fpu_last_seen
    ON film_prehrajto_uploads (last_seen_at)
    WHERE is_alive;

-- Unikátní upload_id napříč celou tabulkou — stejný prehraj.to upload nesmí
-- patřit dvěma různým filmům. V praxi to znamená, že pokud se dva cluster
-- klíče zkolabují, import si musí vybrat jeden film_id a druhý odmítnout.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fpu_upload_id
    ON film_prehrajto_uploads (upload_id);

-- Rollup flagy a preferovaný upload per film. Dva nové SK flagy + pointer
-- na primární upload; CZ flagy reuseujeme existující `prehrajto_has_dub` /
-- `prehrajto_has_subs` (viz komentář nahoře).
ALTER TABLE films
    ADD COLUMN IF NOT EXISTS prehrajto_primary_upload_id TEXT,
    ADD COLUMN IF NOT EXISTS prehrajto_has_sk_dub  BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS prehrajto_has_sk_subs BOOLEAN NOT NULL DEFAULT false;
