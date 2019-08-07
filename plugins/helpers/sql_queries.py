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
                AND events.length = songs.duration""")

    user_table_insert = ("""
    INSERT INTO {}
    (
    SELECT distinct userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
    );""")

    song_table_insert = ("""
    INSERT INTO {}
    (
    SELECT distinct song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE SONG_ID IS NOT NULL
    );""")

    artist_table_insert = ("""
    INSERT INTO {}
    (
    SELECT distinct artist_id, artist_name,artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    );""")

    time_table_insert = ("""
INSERT INTO time
(
SELECT 
distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', 
EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'), 
EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')
FROM staging_events
WHERE page = 'NextSong'
);""")