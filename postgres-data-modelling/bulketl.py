import os
import glob
import psycopg2
import pandas as pd
import configparser
from io import StringIO
from sql_queries import artist_table_insert_from_temp, song_table_insert_from_temp, time_table_insert_from_temp, user_table_insert_from_temp

def process_song_file(cur, conn, all_files):
    """
    - processes song file  
    - store data into temporary tables
    - insert data into songs and artists from temporary tables
    """
    
    # Initializing song and artist stream
    song_stream = StringIO()
    artist_stream = StringIO()

    for i, datafile in enumerate(all_files, 1):
        dataframe = pd.read_json(datafile, lines=True)
        
        song_data = dataframe[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
        song_stream.write(str(song_data[0]) + "\t" + song_data[1] + "\t" + str(song_data[2]) + "\t" + str(song_data[3]) + "\t" + str(song_data[4]) + "\n")
        
        artist_data = dataframe[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
        artist_stream.write(str(artist_data[0]) + "\t" + artist_data[1] + "\t" + artist_data[2] + "\t" + str(artist_data[3]) + "\t" + str(artist_data[4]) + "\n")
    
    # defining columns for songs and artists
    songs_columns=('song_id', 'title', 'artist_id', 'year', 'duration')
    artist_columns=('artist_id', 'name', 'location', 'latitude', 'longitude')
    
    # resetting the position of streams
    song_stream.seek(0)
    artist_stream.seek(0)

    # copy to temporary tables
    cur.copy_from(song_stream, 'songs_copy_temp', columns=songs_columns)
    cur.copy_from(artist_stream, 'artists_copy_temp', columns=artist_columns)
    
    # copy to songs and artists tables with all unique rows
    cur.execute(song_table_insert_from_temp)
    cur.execute(artist_table_insert_from_temp)

    # commit the data
    conn.commit()

def process_log_file(cur, conn, all_files):
    """
    - processes log file  
    """
    
    # Populating Song and Artists Records
    # open log file and load it into pandas dataframe
    time_stream = StringIO()
    user_stream = StringIO()
    songplay_stream = StringIO()

    for i, datafile in enumerate(all_files, 1):
        df = pd.read_json(datafile, lines=True)

        # filter data by NextSong action
        df = df.loc[df['page'] == 'NextSong']
            
        # convert timestamp column to datetime
        df.ts = pd.to_datetime(df['ts'])
    
        # insert time data records
        time_df = pd.DataFrame().assign(start_time=df['ts'], hour=df['ts'].dt.hour,
        day=df['ts'].dt.dayofweek, week=df['ts'].dt.isocalendar().week, 
        month=df['ts'].dt.month, year=df['ts'].dt.year, weekday=df['ts'].dt.dayofweek > 4)
        
        # building time data in string stream
        for i, row in time_df.iterrows():
            time_stream.write(str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t" + str(row[3]) + "\t" + str(row[4]) + "\t" + str(row[5]) + "\t" + str(row[6]) + "\n")

        # load user table
        user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
        
        # building user data in string stream
        for i, row in user_df.iterrows():
            user_stream.write(str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t" + str(row[3]) + "\t" + str(row[4]) + "\n")
        
        # building songplays data in string stream
        for index, row in df.iterrows():
        
            # get songid and artistid from song and artist tables
            song_select = ("""
                SELECT songs.song_id, songs.artist_id 
                FROM songs 
                JOIN artists ON songs.artist_id = artists.artist_id
                WHERE (title = %s and name = %s and duration = %s);
            """)
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            if row.userId != '' and artistid and songid:
                songplay_stream.write(str(row.ts) + "\t" + str(row.userId) + "\t" + str(row.level) + "\t" + str(songid) + "\t" + str(artistid) + "\t" + str(row.sessionId) + "\t" + row.location + "\t" + row.userAgent + "\n")
    
    
    time_stream.seek(0)
    user_stream.seek(0)
    songplay_stream.seek(0)
    
    # Bulk Copying Time/User/SongPlays data
    time_columns = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    user_columns = ('user_id', 'first_name', 'last_name', 'gender', 'level') 
    songplay_columns = ('start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')
    
    cur.copy_from(time_stream, 'time_copy_temp', columns=time_columns)
    cur.copy_from(user_stream, 'users_copy_temp', columns=user_columns)
    
    cur.execute(time_table_insert_from_temp)
    cur.execute(user_table_insert_from_temp)
    
    cur.copy_from(songplay_stream, 'songplays', columns=songplay_columns)
    
    
    conn.commit()

def process_data(cur, conn, filepath, func):
    """
    - Processes song and user logs files  
    
    - Go over each song and user log file and perform database insertion operations
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    func(cur, conn, all_files)


def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Processes SONGS and LOGS files
    
    - Finally, closes the connection. 
    """
    # Loading Configuration from ConfigParser
    config = configparser.ConfigParser()
    config.read_file(open('postgres.cfg'))
    HOST=config.get("POSTGRES","HOST")
    USER=config.get("POSTGRES","USER")
    PASSWORD=config.get("POSTGRES","PASSWORD")
    SPARKIFY_DB=config.get("POSTGRES","SPARKIFY_DB")

    conn = psycopg2.connect("host=" + HOST + " dbname=" + SPARKIFY_DB + " user=" + USER + " password=" + PASSWORD)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()