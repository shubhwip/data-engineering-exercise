# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGPLAYS (songplay_id int, start_time timestamptz, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS USERS (user_id int, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGS (song_id varchar, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS ARTISTS (artist_id varchar, name varchar, location varchar, latitude float, longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME (start_time timestamptz, hour int, day varchar, week int, month int, year int, weekday varchar);
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
###  "userAgent": "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"",
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

songplay_table_insert = ("""
    INSERT INTO SONGPLAYS (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""")
songplay_record = (1, '1541473967796', 38, "FREE", "SOMZWCG12A8C13C480", "ARD7TVE1187B99BFB1", 38, "San Francisco-Oakland-Hayward, CA", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"")

user_table_insert = ("""
    INSERT into USERS (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s);
""")
user_record = (38, 'Walter', 'Frye', 'M', 'FREE')

song_table_insert = ("""
    INSERT into SONGS (song_id, title, artist_id, year, duration) 
    VALUES(%s, %s, %s, %s, %s);
""")
song_record = ("SOMZWCG12A8C13C480", "I Didn't Mean To", "ARD7TVE1187B99BFB1", 0, 218.93179)

artist_table_insert = ("""
    INSERT into ARTISTS (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s);
""")
# TODO : How to pass default values ?
artist_record = ("ARD7TVE1187B99BFB1", 'Casual', 'California - LA', 0.0, 0.0)

#### Tuesday, 6 November 2018 03:12:47.796
time_table_insert = ("""
    INSERT into TIME (start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);
""")
time_record = ('1541473967796', 3, 'Tuesday', 48, 11, 2018, "true");

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,artist_table_insert,time_table_insert]
insert_table_record = [songplay_record, user_record, song_record, artist_record, time_record]