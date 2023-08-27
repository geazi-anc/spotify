CREATE TABLE IF NOT EXISTS spotify.artists (
    id STRING,
    followers INT,
    genres ARRAY<STRING>,
    name STRING,
    popularity INT
)
CLUSTERED BY (id) INTO 20 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;