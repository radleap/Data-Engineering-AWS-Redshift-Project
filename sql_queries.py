import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
staging_time_drop = "DROP TABLE IF EXISTS staging_time" #adding for time calculations bj

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                event_id      bigint identity(0,1)    not null,
                artist        varchar                 null,
                auth          varchar                 null,
                firstName     varchar                 null,
                gender        varchar                 null,
                itemInSession varchar                 null,
                lastName      varchar                 null,
                length        varchar                 null,
                level         varchar                 null,
                location      varchar                 null,
                method        varchar                 null,
                page          varchar                 null,
                registration  varchar                 null,
                sessionId     integer                 not null sortkey distkey,
                song          varchar                 null,
                status        integer                 null,
                ts            bigint                  not null,
                userAgent     varchar                 null,
                userId        integer                 null);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           integer         null,
                artist_id           varchar         not null sortkey distkey,
                artist_latitude     varchar         null,
                artist_longitude    varchar         null,
                artist_location     varchar(250)    null,
                artist_name         varchar(250)    null,
                song_id             varchar         not null,
                title               varchar(250)    null,
                duration            decimal(10)     null,
                year                integer         null);""")

#adding for time calculations bj
staging_time_table_create = ("""CREATE TABLE IF NOT EXISTS staging_time
                    (start_time numeric UNIQUE sortkey, date_new timestamp default NULL);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                         (songplay_id INT IDENTITY(0,1) sortkey, timestamp numeric not null, user_id varchar not null,
                          level varchar, session_id int, location varchar,
                          user_agent varchar, song_id varchar, artist_id varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                    (user_id varchar PRIMARY KEY sortkey, first_name varchar,
                    last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                    (song_id varchar PRIMARY KEY sortkey, title varchar not null,
                    artist_id varchar, year int, duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                    (artist_id varchar PRIMARY KEY sortkey, name varchar not null,
                    location varchar, latitude numeric, longitude varchar)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                    (start_time numeric UNIQUE sortkey, hour numeric, day numeric, week numeric,
                    month numeric, year numeric, weekday numeric);""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {} STATUPDATE ON
    region 'us-west-2'
    """).format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs FROM {} credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2'
    """).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (timestamp,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
                            SELECT  e.ts/1000 as start_time,
                                    e.userId as user_id,
                                    e.level as level,
                                    s.song_id as song_id,
                                    s.artist_id as artist_id,
                                    e.sessionId as session_id,
                                    e.location as location,
                                    e.userAgent as user_agent
                            FROM staging_events as e
                            JOIN staging_songs as s
                                ON (e.artist = s.artist_name)
                            WHERE e.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (                 user_id,
                                                            first_name,
                                                            last_name,
                                                            gender,
                                                            level)
                        SELECT  userId                      AS user_id,
                                firstName                   AS firstName,
                                lastName                    AS lastName,
                                gender                      AS gender,
                                level                       AS level
                        FROM staging_events
                        WHERE page = 'NextSong';""")

song_table_insert = ("""INSERT INTO songs (                 song_id,
                                                            title,
                                                            artist_id,
                                                            year,
                                                            duration)
                        SELECT  song_id                     AS song_id,
                                title                       AS title,
                                artist_id                   AS artist_id,
                                year                        AS year,
                                duration                    AS duration
                        FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (                 artist_id,
                                                                name,
                                                                location,
                                                                latitude,
                                                                longitude)
                            SELECT  artist_id                   AS artist_id,
                                    artist_name                 AS name,
                                    artist_location             AS location,
                                    artist_latitude             AS latitude,
                                    artist_longitude            AS longitude
                            FROM staging_songs;""")

staging_time_table_insert = ("""INSERT INTO staging_time (start_time, date_new)
                                SELECT ts AS start_time, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as date_new
                                FROM staging_events;""")

time_table_insert = ("""INSERT INTO time (                  start_time,
                                                            hour,
                                                            day,
                                                            week,
                                                            month,
                                                            year,
                                                            weekday)
                        SELECT start_time/1000              AS start_time,
                        EXTRACT(HOUR FROM  date_new)        AS hour,
                        EXTRACT(DAY FROM  date_new)         AS day,
                        EXTRACT(WEEK FROM  date_new)        AS week,
                        EXTRACT(MONTH FROM  date_new)       AS month,
                        EXTRACT(YEAR FROM  date_new)        AS year,
                        EXTRACT(DOW FROM  date_new)         AS weekday
                        FROM staging_time;""")


# QUERY LISTS  (adding staging_time_drop for time calculations bj)
create_table_queries = [staging_events_table_create, staging_songs_table_create, staging_time_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_time_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, staging_time_table_insert, time_table_insert]
