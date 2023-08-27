WITH tracks AS (
    SELECT
        name,
        duration_ms,
        id_artist,
        popularity
    FROM
        spotify.tracks LATERAL VIEW explode(id_artists) et AS id_artist
),

max_artist_followers AS (
    SELECT max(followers) AS most_followed
    FROM
        spotify.artists
)

SELECT
    a.name AS artist_name,
    a.followers AS total_followers,
    first_value(t.name) OVER (
        PARTITION BY a.name
        ORDER BY
            t.popularity DESC
    ) AS most_popular_track
FROM
    spotify.artists AS a,
    tracks AS t,
    max_artist_followers AS maf
WHERE
    a.id = t.id_artist
    AND a.followers = maf.most_followed
ORDER BY
    total_followers DESC
LIMIT
    1;
