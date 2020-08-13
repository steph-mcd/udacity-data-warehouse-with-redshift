import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
print(DWH_ROLE_ARN)

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR,
        session_item INT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        session_id INT,
        song VARCHAR ,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR,
        artist_location VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                        (songplay_id INT IDENTITY(0, 1) NOT NULL PRIMARY KEY, 
                        start_time timestamp, 
                        user_id varchar, 
                        level varchar,
                        song_id varchar, 
                        artist_id varchar, 
                        session_id varchar, 
                        location varchar, 
                        user_agent varchar) 
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users 
                        (user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar, latitude decimal, longitude decimal)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
                        (song_id varchar, title varchar, artist_id varchar, year int, duration decimal)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
                            (artist_id varchar, name varchar, location varchar, latitude decimal, longitude decimal)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time
                        (start_time timestamp, hour int, day int, week int, month varchar, year int, weekday varchar)
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events from 's3://udacity-dend/log_data'
                            credentials 'aws_iam_role={}'
                            compupdate off region 'us-west-2'
                            JSON 's3://udacity-dend/log_json_path.json'
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""COPY staging_songs from 's3://udacity-dend/song_data'
                            credentials 'aws_iam_role={}'
                            compupdate off region 'us-west-2'
                            FORMAT AS JSON 'auto'
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
         TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second '
         , user_id
         , level
         , song_id
         , artist_id
         , session_id
         , location
         , user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)    
""")

user_table_insert = (""" 
    insert into users (user_id, first_name, last_name, gender, level) 
    SELECT 
        user_id
        , first_name
        , last_name
        , gender
        , level
    FROM staging_events
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration) 
    SELECT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude) 
    SELECT
        artist_id
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, month, week, year, weekday) 
    SELECT DISTINCT (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ') as ts_start_time,
           EXTRACT(HOUR FROM ts_start_time),
           EXTRACT(DAY FROM ts_start_time),
           EXTRACT(WEEK FROM ts_start_time),
           EXTRACT(MONTH FROM ts_start_time),
           EXTRACT(YEAR FROM ts_start_time),
           EXTRACT(DOW FROM ts_start_time)
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
