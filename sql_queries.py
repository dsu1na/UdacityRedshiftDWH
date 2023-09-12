"""
This script contains all the required SQL queries for the ETL process.
Queries for dropping table, creating table, inserting into table and sample select queries.
"""

import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# path to songs data
song_s3_path = config['S3']['SONG_DATA']
event_s3_path = config['S3']['LOG_DATA']
redshift_s3_role = config['IAM_ROLE']['ARN']
event_jsonpath = config['S3']['LOG_JSONPATH']

# Queries for dropping tables

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# Queries for creating tables

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    "artist" VARCHAR(100),
    "auth" VARCHAR(30),
    "firstname" VARCHAR(30),
    "gender" VARCHAR(10),
    "iteminsession" INT,
    "lastname" VARCHAR(30),
    "length" DECIMAL(15,5),
    "level" VARCHAR(10),
    "location" VARCHAR(100),
    "method" VARCHAR(10),
    "page" VARCHAR(20),
    "registration" BIGINT,
    "sessionid" INT,
    "song" VARCHAR(100),
    "status" INT,
    "ts" BIGINT,
    "useragent" VARCHAR(100),
    "userid" INT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    "num_songs" INT,
    "artist_id" VARCHAR(30),
    "artist_latitude" DECIMAL(15,5),
    "artist_longitude" DECIMAL(15,5),
    "artist_location" VARCHAR(100),
    "artist_name" VARCHAR(100),
    "song_id" VARCHAR(30),
    "title" VARCHAR(30),
    "duartion" DECIMAL(15,5),
    "year" INT
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    "songplay_id" BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    "start_time" TIMESTAMP REFERENCES time(start_time),
    "user_id" INT REFERENCES users(user_id),
    "level" VARCHAR(10),
    "song_id" VARCHAR(30) REFERENCES songs(song_id),
    "artist_id" VARCHAR(30) REFERENCES artists(artist_id),
    "session_id" BIGINT,
    "location" VARCHAR(100),
    "user_agent" VARCHAR(100)
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    "user_id" INT NOT NULL PRIMARY KEY,
    "first_name" VARCHAR(30),
    "last_name" VARCHAR(30),
    "gender" VARCHAR(10),
    "level" VARCHAR(10)
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    "song_id" VARCHAR(30) NOT NULL PRIMARY KEY,
    "title" VARCHAR(30),
    "artist_id" VARCHAR(30) REFERENCES artists(artist_id),
    "year" INT,
    "duration" DECIMAL(15,5)
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    "artist_id" VARCHAR(30) NOT NULL PRIMARY KEY,
    "name" VARCHAR(100),
    "location" VARCHAR(100),
    "lattitude" DECIMAL(15,5),
    "longitude" DECIMAL(15,5)
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    "start_time" TIMESTAMP NOT NULL PRIMARY KEY,
    "hour" INT,
    "day" INT,
    "week" INT,
    "month" INT,
    "year" INT,
    "weekday" INT
)
"""

# Query for copying data from S3 to staging tables

staging_events_copy = """
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json {} 
REGION 'us-east-2'
""".format(event_s3_path, redshift_s3_role, event_jsonpath)

staging_songs_copy = """
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json
REGION 'us-east-2'
""".format(song_s3_path, redshift_s3_role)


# Query for inserting into final tables

songplay_table_insert = """
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)

SELECT
    TO_TIMESTAMP(ev_stg.ts),
    ev_stg.userId,
    ev_stg.level,
    song_stg.song_id,
    song_stg.artist_id,
    ev_stg.sessionId,
    ev_stg.location,
    ev_stg.userAgent
FROM events_staging ev_stg
INNER JOIN staging_songs song_stg 
    ON ev_stag.song = song_stg.title AND ev_stg.artist = song_stg.artist
WHERE page='NextSong'
"""

user_table_insert = """
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)

SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM events_staging
WHERE page='NextSong' 
    AND userId IS NOT NULL
"""

song_table_insert = """
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duartion
)

SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM songs_staging
WHERE song_id IS NOT NULL
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)

SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitide,
    artist_longitude
FROM songs_staging
WHERE artist_id IS NOT NULL
"""

time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)

SELECT DISTINCT
    TO_TIMESTAMP(ts),
    EXTRACT(hour from TO_TIMESTAMP(ts)),
    EXTRACT(day from TO_TIMESTAMP(ts)),
    EXTRACT(week from TO_TIMESTAMP(ts)),
    EXTRACT(month from TO_TIMESTAMP(ts)),
    EXTRACT(year from TO_TIMESTAMP(ts)),
    EXTRACT(dow from TO_TIMESTAMP(ts))
FROM events_staging
WHERE page='NextPage'
    AND ts IS NOT NULL
"""


# Query lists for execution

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_table_create, staging_songs_table_create]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]