import configparser
import os

# DROP TABLES QUERIES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS CASCADE"
user_table_drop = "DROP TABLE IF EXISTS USERS CASCADE"
song_table_drop = "DROP TABLE IF EXISTS SONGS CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS CASCADE"
time_table_drop = "DROP TABLE IF EXISTS TIME CASCADE"

# CREATE TABLES QUERIES

# Todo : Follow consistent naming convention for table names
staging_events_table_create= ("""
CREATE TABLE STAGING_EVENTS 
(
        ARTIST          VARCHAR(200),
        AUTH            VARCHAR(200),
        FIRSTNAME       VARCHAR(200),
        GENDER          CHARACTER,
        ITEMINSESSION   INTEGER,
        LASTNAME        VARCHAR(200),
        LENGTH          FLOAT,
        LEVEL           VARCHAR(200),
        LOCATION        VARCHAR(200),
        METHOD          VARCHAR(200),
        PAGE            VARCHAR(200),
        REGISTRATION    FLOAT,
        SESSIONID       INTEGER,
        SONG            VARCHAR(200),
        STATUS          INTEGER,
        TS              TIMESTAMP,
        USERAGENT       VARCHAR(200),           
        USERID          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE STAGING_SONGS 
(
        NUM_SONGS           INTEGER      ,
        ARTIST_ID           VARCHAR(200) ,
        ARTIST_LATITUDE     FLOAT        ,
        ARTIST_LONGITUDE    FLOAT        ,
        ARTIST_LOCATION     VARCHAR(200) ,
        ARTIST_NAME         VARCHAR(200) ,
        SONG_ID             VARCHAR(200) ,   
        TITLE               VARCHAR(200) ,
        DURATION            FLOAT        ,
        YEAR                INTEGER     
);
""")

songplay_table_create = ("""
    CREATE TABLE SONGPLAYS (
        SONGPLAY_ID INTEGER IDENTITY(0,1),
        USER_ID INTEGER,
        SONG_ID VARCHAR,
        ARTIST_ID VARCHAR,
        START_TIME TIMESTAMP,
        SESSION_ID INTEGER,
        LEVEL VARCHAR,
        LOCATION VARCHAR,
        USER_AGENT VARCHAR,
        PRIMARY KEY (SONGPLAY_ID),
        FOREIGN KEY (USER_ID) REFERENCES USERS (USER_ID),
        FOREIGN KEY (SONG_ID) REFERENCES SONGS (SONG_ID),
        FOREIGN KEY (ARTIST_ID) REFERENCES ARTISTS (ARTIST_ID),
        FOREIGN KEY (START_TIME) REFERENCES TIME (START_TIME)
    );
""")

user_table_create = ("""
    CREATE TABLE USERS (
        USER_ID INTEGER NOT NULL,
        FIRST_NAME VARCHAR NOT NULL,
        LAST_NAME VARCHAR,
        GENDER VARCHAR,
        LEVEL VARCHAR,
        PRIMARY KEY (USER_ID)
    );
""")

# Question : Should the ARTIST_ID be foreign key here or not ?
# I think it should not be because what if it is null for song
song_table_create = ("""
    CREATE TABLE SONGS (
        SONG_ID VARCHAR(20),
        ARTIST_ID VARCHAR(20) ,
        TITLE VARCHAR,
        YEAR INTEGER,
        DURATION INTEGER,
        PRIMARY KEY (SONG_ID),
        FOREIGN KEY (ARTIST_ID) REFERENCES ARTISTS (ARTIST_ID)
    );
""")

artist_table_create = ("""
    CREATE TABLE ARTISTS (
        ARTIST_ID VARCHAR(20),
        NAME VARCHAR,
        LOCATION VARCHAR,
        LATITUDE FLOAT,
        LONGITUDE FLOAT,
        PRIMARY KEY (ARTIST_ID)
    );
""")

time_table_create = ("""
    CREATE TABLE TIME (
        START_TIME  TIMESTAMP,
        HOUR        INTEGER,
        DAY         VARCHAR,
        WEEK        INTEGER,
        MONTH       INTEGER,
        YEAR        INTEGER,
        WEEKDAY     BOOLEAN,
        PRIMARY KEY (START_TIME)
    );
""")

config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE", "arn")
DWH_STAGING_DATA_FORMAT = config.get("CLUSTER","dataformat")
DWH_CLUSTER_REGION = config.get("CLUSTER","region")
S3_LOG_DATA_LOCATION = config.get("S3", "log_data")
S3_SONG_DATA_LOCATION = config.get("S3", "song_data")

# STAGING TABLES COPY QUERIES
staging_events_copy = ("""
    copy staging_events from {}
    iam_role '{}' 
    {} 'auto ignorecase' compupdate off region '{}'
    TIMEFORMAT as 'epochmillisecs';
""").format(S3_LOG_DATA_LOCATION, DWH_ROLE_ARN, DWH_STAGING_DATA_FORMAT, DWH_CLUSTER_REGION)
 
staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}' 
    {} 'auto ignorecase' compupdate off region '{}'
    TIMEFORMAT as 'epochmillisecs';
""").format(S3_SONG_DATA_LOCATION, DWH_ROLE_ARN, DWH_STAGING_DATA_FORMAT, DWH_CLUSTER_REGION)

# FINAL TABLES INSERT QUERIES

songplay_table_insert = ("""
INSERT INTO SONGPLAYS (USER_ID, SONG_ID, ARTIST_ID, START_TIME, SESSION_ID, LEVEL, LOCATION, USER_AGENT)
    SELECT E.USERID         AS USER_ID,
           S.SONG_ID        AS SONG_ID,
           S.ARTIST_ID      AS ARTIST_ID, 
           E.TS             AS START_TIME, 
           E.SESSIONID      AS SESSION_ID, 
           E.LEVEL          AS LEVEL, 
           E.LOCATION       AS LOCATION, 
           E.USERAGENT      AS USER_AGENT
    FROM STAGING_EVENTS AS E
    LEFT OUTER JOIN STAGING_SONGS AS S ON ( E.ARTIST = S.ARTIST_NAME AND E.SONG = S.TITLE AND E.LENGTH = S.DURATION )
    WHERE E.USERID IS NOT NULL AND E.PAGE = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO USERS (USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL)
    SELECT DISTINCT m.USERID    AS USER_ID,
           m.FIRSTNAME          AS FIRST_NAME,
           m.LASTNAME           AS LAST_NAME, 
           m.GENDER             AS GENDER,
           m.LEVEL              AS LEVEL
    FROM STAGING_EVENTS m
    WHERE m.USERID IS NOT NULL AND ts = (select max(ts) FROM staging_events s WHERE s.userId = m.userId)
    ORDER BY m.userId DESC 
""")

song_table_insert = ("""
INSERT INTO SONGS (SONG_ID, ARTIST_ID, TITLE, YEAR, DURATION)
    SELECT DISTINCT S.SONG_ID   AS SONG_ID,
           S.ARTIST_ID          AS ARTIST_ID,
           S.TITLE              AS TITLE, 
           S.YEAR               AS YEAR, 
           S.DURATION           AS DURATION
    FROM STAGING_SONGS AS S
    WHERE S.SONG_ID IS NOT NULL AND S.SONG_ID NOT IN (SELECT DISTINCT (SONG_ID) FROM SONGS)      
""")

artist_table_insert = ("""
INSERT INTO ARTISTS (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
    SELECT DISTINCT S.ARTIST_ID        AS ARTIST_ID,
           S.ARTIST_NAME               AS NAME, 
           S.ARTIST_LOCATION           AS LOCATION, 
           S.ARTIST_LATITUDE           AS LATITUDE,
           S.ARTIST_LONGITUDE          AS LONGITUDE
    FROM STAGING_SONGS AS S
    WHERE S.ARTIST_ID IS NOT NULL AND S.ARTIST_ID NOT IN (SELECT DISTINCT(ARTIST_ID) FROM ARTISTS)  
""")

time_table_insert = ("""
INSERT INTO TIME (START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY) 
    SELECT DISTINCT ts                                          AS START_TIME,
           EXTRACT(hour FROM ts)                                AS hour, 
           EXTRACT(day FROM ts)                                 AS day, 
           EXTRACT(week FROM ts)                                AS week, 
           EXTRACT(month FROM ts)                               AS month, 
           EXTRACT(year FROM ts)                                AS year, 
           CASE WHEN EXTRACT(dayofweek FROM ts) IN (6, 7) THEN true ELSE false END AS weekday
    FROM STAGING_EVENTS AS S
    WHERE S.TS IS NOT NULL AND S.TS NOT IN (SELECT DISTINCT (START_TIME) FROM TIME)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, user_table_insert, song_table_insert, time_table_insert, songplay_table_insert]
