WITH tracks AS (
    SELECT
        id_artist,
        name,
        popularity,
        date_format(release_date, 'yyyy') AS release_year
    FROM
        spotify.tracks
            LATERAL VIEW explode(id_artists) exploded_table AS id_artist
),

artists AS (
    SELECT
        a.id AS id_artist,
        a.name AS artist_name,
        t.popularity AS track_popularity,
        t.release_year AS track_release_year
    FROM
        spotify.artists AS a,
        tracks AS t
    WHERE
        a.id = t.id_artist
        AND t.release_year IS NOT NULL
)

SELECT
    a.artist_name,
    a.track_release_year AS year,
    sum(a.track_popularity) AS total_tracks_popularity
FROM
    artists AS a
GROUP BY
    a.id_artist,
    a.artist_name,
    a.track_release_year
ORDER BY
    total_tracks_popularity DESC
LIMIT
    3;
