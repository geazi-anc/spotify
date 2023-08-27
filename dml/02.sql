WITH tracks AS (
    SELECT
        t.name,
        t.duration_ms,
        id_artist
    FROM
        spotify.tracks AS t LATERAL VIEW explode(id_artists) et AS id_artist
),

artists AS (
    SELECT
        a.name AS artist_name,
        t.name AS track_name,
        t.duration_ms AS track_duration_ms
    FROM
        spotify.artists AS a,
        tracks AS t
    WHERE
        a.id = t.id_artist
),

average_tracks_duration AS (
    SELECT int(avg(a.track_duration_ms)) AS duration
    FROM
        artists AS a
)

SELECT DISTINCT
    a.artist_name AS artist_name,
    round(a.track_duration_ms / 1000, 2) AS track_duration_s
FROM
    artists AS a,
    average_tracks_duration AS atd
WHERE
    a.track_duration_ms BETWEEN atd.duration
    AND atd.duration + atd.duration * 0.5
ORDER BY
    track_duration_s DESC
LIMIT
    10;
