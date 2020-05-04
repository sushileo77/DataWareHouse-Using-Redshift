import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id      BIGINT IDENTITY(0,1)    NOT NULL,
                artist        VARCHAR ,
                auth          VARCHAR ,
                firstName     VARCHAR ,
                gender        VARCHAR ,
                itemInSession VARCHAR  NOT NULL,
                lastName      VARCHAR ,
                length        VARCHAR ,
                level         VARCHAR NOT NULL,
                location      VARCHAR ,
                method        VARCHAR NOT NULL,
                page          VARCHAR  ,
                registration  NUMERIC ,
                sessionId     INTEGER NOT NULL,
                song          VARCHAR,
                status        INT NOT NULL,
                ts            NUMERIC NOT NULL,
                userAgent     VARCHAR ,
                userId        INT 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INT NOT NULL,
                artist_id           VARCHAR NOT NULL,
                artist_latitude     VARCHAR ,
                artist_longitude    VARCHAR ,
                artist_location     VARCHAR NOT NULL,
                artist_name         VARCHAR NOT NULL,
                song_id             VARCHAR NOT NULL,
                title               VARCHAR NOT NULL,
                duration            NUMERIC NOT NULL,
                year                INT     NOT NULL
    );
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role '{}'
    format as json {}
    """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    format as json 'auto'
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_create = ("""
    create table songplays (
        songplay_id int identity(0, 1) primary key,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT NOT NULL,
        location VARCHAR,
        user_agent VARCHAR NOT NULL
    )
""")

user_table_create = ("""
    create table users (
        user_id int primary key,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    create table songs (
        song_id VARCHAR primary key,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT NOT NULL,
        duration NUMERIC NOT NULL
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id char (18) primary key,
        name VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    create table times (
        start_time numeric primary key,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    )
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (             
                                start_time,
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (                 user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userId          AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")
# QUERY LISTS
create_table_queries  = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries    = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries    = [staging_events_copy, staging_songs_copy]
insert_table_queries  = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]