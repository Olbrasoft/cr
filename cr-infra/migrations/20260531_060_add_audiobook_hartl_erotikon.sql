INSERT INTO audiobooks (title, author, narrator, year, duration, archive_id, cover_filename)
VALUES
    ('Malý pražský erotikon', 'Patrik Hartl', 'David Novotný', 2024, '16h 6m', 'patrik-hartl-maly-prazsky-erotikon-cz', 'cover.jpg')
ON CONFLICT (archive_id) DO NOTHING;
