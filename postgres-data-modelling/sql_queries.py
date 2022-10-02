# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
user_copy_temp_table_drop = "DROP TABLE IF EXISTS USERS_COPY_TEMP"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
song_copy_temp_table_drop = "DROP TABLE IF EXISTS SONGS_COPY_TEMP"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
artist_copy_temp_table_drop = "DROP TABLE IF EXISTS ARTISTS_COPY_TEMP"
time_table_drop = "DROP TABLE IF EXISTS TIME"
time_copy_temp_table_drop = "DROP TABLE IF EXISTS TIME_COPY_TEMP"

# CREATE TABLES STATEMENTS
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGPLAYS (
        songplay_id SERIAL PRIMARY KEY,
        start_time timestamp NOT NULL REFERENCES TIME (start_time),
        user_id int NOT NULL REFERENCES USERS (user_id), 
        level varchar, 
        song_id varchar NOT NULL REFERENCES SONGS (song_id), 
        artist_id varchar NOT NULL REFERENCES ARTISTS (artist_id), 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS USERS (
        user_id int PRIMARY KEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    )
""")

# Temporary table is created with no unique constraints
# To perform COPY operation in `bulketl.py` added the temporary table
# Later we extract data from this table and load it into main USERS table discarding duplicates
user_copy_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS USERS_COPY_TEMP (
        user_id int, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGS (
        song_id varchar PRIMARY KEY, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float
    )
""")

# Temporary table is created with no unique constraints
# To perform COPY operation in `bulketl.py` added the temporary table
# Later we extract data from this table and load it into main SONGS table discarding duplicates
song_copy_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGS_COPY_TEMP (
        song_id varchar, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS ARTISTS (
        artist_id varchar PRIMARY KEY, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float
    )
""")

# Temporary table is created with no unique constraints
# To perform COPY operation in `bulketl.py` added the temporary table
# Later we extract data from this table and load it into main ARTISTS table discarding duplicates
artist_copy_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS ARTISTS_COPY_TEMP (
        artist_id varchar, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME (
        start_time timestamp PRIMARY KEY, 
        hour int, 
        day varchar, 
        week int, 
        month int, 
        year int, 
        weekday varchar
    )
""")

# Temporary table is created with no unique constraints
# To perform COPY operation in `bulketl.py` added the temporary table
# Later we extract data from this table and load it into main TIME table discarding duplicates
time_copy_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME_COPY_TEMP (
        start_time timestamp, 
        hour int, 
        day varchar, 
        week int, 
        month int, 
        year int, 
        weekday varchar
    )
""")

# INSERT RECORDS

### Example Taken from Sample Data
###{
###  "artist": null,
###  "auth": "Logged In",
###  "firstName": "Walter",
###  "gender": "M",
###  "itemInSession": 0,
###  "lastName": "Frye",
###  "length": null,
###  "level": "free",
###  "location": "San Francisco-Oakland-Hayward, CA",
###  "method": "GET",
###  "page": "Home",
###  "registration": 1540919166796,
###  "sessionId": 38,
###  "song": null,
###  "status": 200,
###  "ts": 1541105830796,
###  "userAgent": "\"Mozilla/5.0 (Macintosh Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"",
###  "userId": "39"
###}

###{
###  "num_songs": 1,
###  "artist_id": "ARD7TVE1187B99BFB1",
###  "artist_latitude": null,
###  "artist_longitude": null,
###  "artist_location": "California - LA",
###  "artist_name": "Casual",
###  "song_id": "SOMZWCG12A8C13C480",
###  "title": "I Didn't Mean To",
###  "duration": 218.93179,
###  "year": 0
###}

# INSERT statement for songplay table
songplay_table_insert = ("""
    INSERT INTO SONGPLAYS (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")
# Dummy record for songplay table
songplay_record = (1541473967796, 38, "FREE", "SOMZWCG12A8C13C480", "ARD7TVE1187B99BFB1", 38, "San Francisco-Oakland-Hayward, CA", "\"Mozilla/5.0 (Macintosh Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"")

# INSERT statement for user table
user_table_insert = ("""
    INSERT into USERS (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING
""")

# ETL Query to load data from temporary table and load it into USERS table
# Discarding duplicates maintaining data integrity
# This is used in bulketl.py
user_table_insert_from_temp = ("""
    INSERT into USERS (user_id, first_name, last_name, gender, level) 
        SELECT user_id, first_name, last_name, gender, level
        FROM USERS_COPY_TEMP AS U
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")
# Dummy record for user table
user_record = (38, 'Walter', 'Frye', 'M', 'FREE')

# INSERT statement for song table
song_table_insert = ("""
    INSERT into SONGS (song_id, title, artist_id, year, duration) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

# ETL Query to load data from temporary table and load it into SONGS table
# Discarding duplicates maintaining data integrity
# This is used in bulketl.py
song_table_insert_from_temp = ("""
    INSERT into SONGS (song_id, title, artist_id, year, duration) 
        SELECT song_id, title, artist_id, year, duration
        FROM SONGS_COPY_TEMP AS S
    ON CONFLICT (song_id) DO NOTHING
""")

# Dummy record for song table
song_record = ("SOMZWCG12A8C13C480", "I Didn't Mean To", "ARD7TVE1187B99BFB1", 0, 218.93179)

# INSERT statement for artist table
artist_table_insert = ("""
    INSERT into ARTISTS (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")

# ETL Query to load data from temporary table and load it into ARTISTS table
# Discarding duplicates maintaining data integrity
# This is used in bulketl.py
artist_table_insert_from_temp = ("""
    INSERT into ARTISTS (artist_id, name, location, latitude, longitude) 
        SELECT artist_id, name, location, latitude, longitude
        FROM ARTISTS_COPY_TEMP AS A
    ON CONFLICT (artist_id) DO NOTHING
""")
# Dummy record for artist table
artist_record = ("ARD7TVE1187B99BFB1", 'Casual', 'California - LA', None, None)

#### Tuesday, 6 November 2018 03:12:47.796
# INSERT statement for time table
time_table_insert = ("""
    INSERT into TIME (start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
""")

# ETL Query to load data from temporary table and load it into TIME table
# Discarding duplicates maintaining data integrity
# This is used in bulketl.py
time_table_insert_from_temp = ("""
    INSERT into TIME (start_time, hour, day, week, month, year, weekday) 
        SELECT start_time, hour, day, week, month, year, weekday
        FROM TIME_COPY_TEMP AS T
    ON CONFLICT (start_time) DO NOTHING
""")
# Dummy record for time table
time_record = (1541473967796, 3, 'Tuesday', 48, 11, 2018, "true")

# FIND SONGS
song_select = ("""
""")

# QUERY LISTS
create_table_queries = [user_table_create, user_copy_temp_table_create, song_table_create, song_copy_temp_table_create, artist_table_create, artist_copy_temp_table_create, time_table_create, time_copy_temp_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, user_copy_temp_table_drop, song_table_drop, song_copy_temp_table_drop, artist_table_drop, song_copy_temp_table_drop, time_table_drop, time_copy_temp_table_drop]
insert_table_queries = [user_table_insert, song_table_insert,artist_table_insert,time_table_insert, songplay_table_insert]
insert_table_record = [user_record, song_record, artist_record, time_record, songplay_record]