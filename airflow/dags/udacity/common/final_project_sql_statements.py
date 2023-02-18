class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    create_staging_events = ("""
        DROP TABLE IF EXISTS staging_events;
        CREATE TABLE IF NOT EXISTS staging_events 
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
                start_time bigint,
                userAgent text,
                userId int
            );
    """)

    create_staging_songs = ("""
        DROP TABLE IF EXISTS staging_songs;
        CREATE TABLE IF NOT EXISTS staging_songs
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





