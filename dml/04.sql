WITH tracks AS (
    SELECT
        t.popularity,
        10 * floor(date_format(release_date, 'yyyy') / 10) AS release_decade
    FROM
        spotify.tracks AS t
    WHERE
        t.release_date IS NOT NULL
)

SELECT
    t.release_decade AS decade,
    count(*) AS total_released_tracks,
    sum(t.popularity) AS total_popularity
FROM
    tracks AS t
GROUP BY
    t.release_decade
ORDER BY
    total_released_tracks DESC,
    total_popularity DESC;
