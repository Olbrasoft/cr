INSERT INTO audiobooks (title, author, narrator, year, duration, archive_id, cover_filename)
VALUES
    ('Povídky a Nové povídky – Komplet', 'Zdeněk Svěrák', 'Zdeněk Svěrák, Daniela Kolářová, Libuše Šafránková', 2016, '8h 47m', 'zdenek-sverak-povidky-komplet-cz', 'cover.jpg')
ON CONFLICT (archive_id) DO NOTHING;
