WITH tracks AS (
    SELECT
        id,
        name,
        popularity,
        duration_ms,
        id_artist,
        10 * floor(date_format(t.release_date, 'yyyy') / 10) AS release_decade
    FROM
        spotify.tracks AS t
            LATERAL VIEW explode(t.id_artists) et AS id_artist
),

artists AS (
    SELECT
        a.id AS id_artist,
        a.name AS artist_name,
        a.popularity AS artist_popularity,
        t.id AS id_track,
        t.name AS track_name,
        t.release_decade AS track_release_decade,
        t.popularity AS track_popularity
    FROM
        spotify.artists AS a,
        tracks AS t
    WHERE
        a.id = t.id_artist
),

popular AS (
    SELECT
        a.track_release_decade,
        max(a.track_popularity) AS most_popular
    FROM
        artists AS a
    GROUP BY
        a.track_release_decade
)

SELECT DISTINCT
    concat(a.track_release_decade, 'S') AS decade,
    first_value(a.track_name)
        OVER (PARTITION BY p.track_release_decade ORDER BY p.most_popular DESC)
        AS most_popular_track,
    p.most_popular AS track_popularity
FROM
    artists AS a,
    popular AS p
WHERE
    a.track_release_decade = p.track_release_decade
    AND a.track_popularity = p.most_popular
ORDER BY
    decade DESC;
