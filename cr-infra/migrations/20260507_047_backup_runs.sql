-- Auto-zálohy produkční PostgreSQL na Cloudflare R2. Každý běh skriptu
-- scripts/backup-db.sh (spouštěný systemd timerem na prod serveru) zapisuje
-- jednu řádku sem — start, konec, status, velikost gzip dumpu, filename
-- v bucketu cr-backups/auto/ a případná chybová hláška.
--
-- Admin UI /admin/backups/ čte z téhle tabulky jako /admin/import/ čte z
-- import_runs — stejný sloupec status, stejné badge.

CREATE TABLE IF NOT EXISTS backup_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    -- 'running' while in progress, 'ok' on success, 'error' on failure.
    status          TEXT NOT NULL DEFAULT 'running',
    -- 'auto' = systemd timer, 'manual' = operator spustil ručně.
    trigger         TEXT NOT NULL DEFAULT 'auto',
    -- Velikost výsledného gzip souboru v bajtech. NULL, pokud pg_dump selhal.
    size_bytes      BIGINT,
    -- Klíč v R2 bucketu, např. 'auto/cr_prod_2026-04-17_0300.dump.gz'.
    dump_filename   TEXT,
    -- Krátká diagnostika pokud status='error'. Detail jde do journald.
    error_message   TEXT,

    CONSTRAINT backup_runs_status_check CHECK (status IN ('running', 'ok', 'error')),
    CONSTRAINT backup_runs_trigger_check CHECK (trigger IN ('auto', 'manual'))
);

-- Default dotaz v admin handleru: ORDER BY started_at DESC LIMIT 30.
CREATE INDEX IF NOT EXISTS backup_runs_started_at_idx
    ON backup_runs (started_at DESC);
