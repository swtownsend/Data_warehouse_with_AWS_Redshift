import configparser


# CONFIG
# to read the dwh config file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# scrits tp drop the staging final tabes if they exist
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# script to create the staging events table
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist TEXT,
                                                                            auth TEXT,
                                                                            firstName TEXT,
                                                                            gender TEXT,
                                                                            itemInSession INT,
                                                                            lastName TEXT,
                                                                            length DECIMAL,
                                                                            level TEXT,
                                                                            location TEXT,
                                                                            method TEXT,
                                                                            page TEXT,
                                                                            registration BIGINT,
                                                                            sessionId INT,
                                                                            song TEXT,
                                                                            status INT,
                                                                            ts BIGINT,
                                                                            userAgent TEXT,
                                                                            userId INT);
""")

# script to create the staging songs table
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (staging_songs int IDENTITY(0,1) PRIMARY KEY,
                                                                           num_songs int,
                                                                           artist_id TEXT NOT NULL,
                                                                           artist_latitude DECIMAL,
                                                                           artist_longitude DECIMAL,
                                                                           artist_location TEXT,
                                                                           artist_name VARCHAR,
                                                                           song_id TEXT NOT NULL,
                                                                           title TEXT NOT NULL,
                                                                           duration DECIMAL,
                                                                           year INT);
""")

#script to create the songplays fact table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays  (songplay_id int IDENTITY(0,1) PRIMARY KEY, 
                                                                   start_time bigint, 
                                                                   user_id int, 
                                                                   level varchar NOT NULL, 
                                                                   song_id varchar, 
                                                                   artist_id varchar, 
                                                                   session_id int NOT NULL, 
                                                                   location varchar, 
                                                                   user_agent varchar)
                                                                   ;""")

# script to create the users demension table
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, 
                                                          first_name varchar NOT NULL, 
                                                          last_name varchar NOT NULL, 
                                                          gender varchar, 
                                                          level varchar NOT NULL);
""")

# script to create the songs demension table
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, 
                                                          title varchar NOT NULL, 
                                                          artist_id varchar NOT NULL, 
                                                          year int, 
                                                          duration DECIMAL );
""")

# script to create the artists demension table
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, 
                                                              name varchar, 
                                                              location varchar, 
                                                              latitude DECIMAL, 
                                                              longitude DECIMAL);
""")

# script to create the time demension table
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY, 
                                                         hour int, 
                                                         day int, 
                                                         week int, 
                                                         month int, 
                                                         year int, 
                                                         weekday int);
""")
# STAGING TABLES

# redshift copy comand to populate the staging_events table
staging_events_copy = ("""
                        COPY staging_events FROM {}
                    CREDENTIALS 'aws_iam_role={}'
                    REGION 'us-west-2'
                    FORMAT as JSON {}
                    """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

# redshift copy comand to populate the staging_songs table
staging_songs_copy = ("""
                    COPY staging_songs FROM {}
                    CREDENTIALS 'aws_iam_role={}'
                    REGION 'us-west-2'
                    FORMAT as JSON 'auto'
                    """).format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

# FINAL TABLES

# script to insert the data into the users demension table
user_table_insert = (""" INSERT INTO users (user_id,first_name,last_name,gender,level)
                         SELECT distinct userId,firstName,LastName,gender,level
                         FROM staging_events
                         WHERE userId IS NOT NULL;
""")

# script to insert the data into the songs demension table
song_table_insert = ("""INSERT INTO songs(song_id, title,artist_id,year,duration)
                        SELECT song_id,title,artist_id,year,duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;    
""")

# script to insert the data into the artists demension table
artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
                          SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")

# script to insert the data into the time demension table
time_table_insert = ("""INSERT INTO time (start_time, hour,day,week,month,year,weekday)
                        Select  distinct ts
                                ,EXTRACT(HOUR FROM start_time) As hour
                                ,EXTRACT(DAY FROM start_time) As day
                                ,EXTRACT(WEEK FROM start_time) As week
                                ,EXTRACT(MONTH FROM start_time) As month
                                ,EXTRACT(YEAR FROM start_time) As year
                                ,EXTRACT(DOW FROM start_time) As weekday
                        FROM (
                        SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
                        FROM staging_events
                        );                        
""")

# script to insert the data into the songplays fact table
songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)                              
                            SELECT ts,userId,level,song_id,artist_id,sessionId,location,userAgent
                            FROM (                             
							SELECT se.ts, se.userId, se.level, sa.song_id, sa.artist_id, se.sessionId, se.location, se.userAgent 
                            FROM staging_events se
                              JOIN 
                                (SELECT songs.song_id, artists.artist_id, songs.title, artists.name,songs.duration 
                                 FROM songs 
                                 JOIN artists 
                                    ON songs.artist_id = artists.artist_id) AS sa
                                ON (sa.title = se.song 
                                AND sa.name = se.artist 
                                AND sa.duration = se.length)
                            WHERE se.page = 'NextSong');                        
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
