class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplay_table (start_time, userid, level, song_id, artist_id, sessionid, location, useragent)
        SELECT
            to_timestamp(ts, 'YYYYMMDD HHMISS') AS start_time,
            userid AS user_id,
            level,
            song_id,
            artist_id,
            sessionid,
            location,
            useragent
        FROM staging_events_table e
        LEFT JOIN staging_songs_table s
        ON (e.artist = s.artist_name)
        AND (e.length = s.duration)
        AND (e.song = s.title)
        AND page = 'NextSong';
    """)

 
    user_table_insert = ("""
        INSERT INTO user_table (userid, firstname, lastname, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events_table
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO song_table (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs_table
    """)

    artist_table_insert = ("""
        INSERT INTO artist_table (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs_table
    """)

    time_table_insert = ("""
        INSERT INTO time_table (start_time, hour, day, week, month, year, dayofweek)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplay_table
    """)

    create_staging_events = ("""
        DROP TABLE IF EXISTS staging_events_table;
        CREATE TABLE IF NOT EXISTS staging_events_table 
            (
                artist text, 
                auth text,
                firstname text,
                gender text,
                iteminsession int4,
                lastname text,
                length float,
                level text,
                location text,
                method text,
                page text,
                registration float,
                sessionid int4,
                song text,
                status int4,
                ts bigint,
                useragent text,
                userId int
            );
    """)

    create_staging_songs = ("""
        DROP TABLE IF EXISTS staging_songs_table;
        CREATE TABLE IF NOT EXISTS staging_songs_table
            (
                num_songs int4,
                artist_id text,
                artist_latitude float,
                artist_longitude float,
                artist_location text,
                artist_name text,
                song_id text,
                title text,
                duration float,
                year int

            );
    """)

    # CREATE FACT - song play
    # Create song play query
    create_songplay_table = ("""
        DROP TABLE IF EXISTS songplay_table;
        CREATE TABLE IF NOT EXISTS songplay_table
        (
            songplay_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            start_time timestamp REFERENCES time_table,
            userid INT4 REFERENCES user_table,
            level TEXT,
            song_id TEXT REFERENCES song_table,
            artist_id TEXT REFERENCES artist_table,
            sessionid int4,
            location TEXT,
            useragent TEXT
        );
    """)

    # CREATE DIMENSIONAL TABLES - user, song, artist, and time tables
    # Create User Table
    create_user_table = ("""
        DROP TABLE IF EXISTS user_table CASCADE;
        CREATE TABLE IF NOT EXISTS user_table
        (
            id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            userid INT4,
            firstname TEXT,
            lastname TEXT,
            gender TEXT,
            level TEXT
        );
    """)

    # Create song table
    create_song_table = ("""
        DROP TABLE IF EXISTS song_table CASCADE;
        CREATE TABLE IF NOT EXISTS song_table
        (
            id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            song_id TEXT,
            title TEXT,
            artist_id TEXT,
            year INT4,
            duration float
        );
    """)

    # Create artist table
    create_artist_table = ("""
        DROP TABLE IF EXISTS artist_table CASCADE;
        CREATE TABLE IF NOT EXISTS artist_table
        (
            id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            artist_id TEXT,
            artist_name TEXT,
            artist_location TEXT,
            artist_latitude float,
            artist_longitude float
        );
    """)

    # Create time table
    create_time_table = ("""
        DROP TABLE IF EXISTS time_table CASCADE;
        CREATE TABLE IF NOT EXISTS time_table
        (
            time_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            start_time TIMESTAMP,
            hour INT4,
            day INT4,
            week INT4,
            month INT4,
            year INT4,
            dayofweek TEXT
        );
    """)

    check_number_of_rows = "SELECT COUNT(*) FROM {}"
   





