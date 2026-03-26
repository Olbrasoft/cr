CREATE TABLE audiobooks (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    author          TEXT NOT NULL,
    narrator        TEXT NOT NULL,
    year            SMALLINT NOT NULL,
    duration        TEXT NOT NULL,
    archive_id      TEXT NOT NULL UNIQUE,
    cover_filename  TEXT NOT NULL DEFAULT 'cover.png',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO audiobooks (title, author, narrator, year, duration, archive_id, cover_filename)
VALUES
    ('Sapiens – Stručné dějiny lidstva', 'Yuval Noah Harari', 'Luboš Ondráček', 2019, '15h 53m', 'yuval-harari-sapiens-cz', 'sapiens.jpg'),
    ('Homo Deus – Stručné dějiny zítřka', 'Yuval Noah Harari', 'Luboš Ondráček', 2020, '14h 18m', 'yuval-harari-homo-deus-cz', 'cover.png');
