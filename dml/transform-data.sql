INSERT OVERWRITE TABLE spotify.tracks
SELECT
    id,
    name,
    popularity,
    release_date,
    duration_ms,
    explicit,
    split(trim(id_artists[0], "[]'"), ",") AS id_artists
FROM
    spotify.tracks;
