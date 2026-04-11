-- #366 — store the human-readable resolution (e.g. "1080p", "720p")
-- alongside the raw yt-dlp `quality` (format_id). The library card
-- badge used to render `quality`, which is source-specific garbage —
-- YouTube integer itags like "137", Streamtape `"mp4"`, TikTok
-- `"bytevc1_720p_1504834-0"` etc. `resolution` is always a clean
-- `<height>p` string and maps 1:1 onto the `bestvideo+bestaudio`
-- output yt-dlp produces, so the UI can show it consistently.
--
-- Existing rows get a best-effort backfill via regex: anything of
-- the form `NNNp` inside the `quality` string wins. Rows where no
-- such substring exists (YouTube numeric itags, `"mp4"`, `"whatsapp"`)
-- stay `NULL` and the card just hides the badge for them.

ALTER TABLE videos ADD COLUMN resolution TEXT;

UPDATE videos
   SET resolution = substring(quality from '(\d+p)')
 WHERE resolution IS NULL
   AND quality ~ '\d+p';
