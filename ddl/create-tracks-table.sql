CREATE TABLE IF NOT EXISTS spotify.tracks (
    id STRING,
    name STRING,
    popularity INT,
    release_date DATE,
    duration_ms BIGINT,
    explicit INT,
    id_artists ARRAY<STRING>
)
CLUSTERED BY (id_artists) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
