import os
import glob
import psycopg2
import pandas as pd
import configparser
from sql_queries import song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert


def process_song_file(cur, filepath):
    """
    - Processes song file  
    
    - Insert songs records and artists records into songs and artists table
    """
    # open song file
    dataframe = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = dataframe[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = dataframe[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Processes user logs file  
    
    - Insert songplays, time and user records
    """
    # open log file and load it into pandas dataframe
    df = pd.read_json(filepath, lines=True)

    # filter data by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_df = pd.DataFrame().assign(start_time=df['ts'], hour=df['ts'].dt.hour,
     day=df['ts'].dt.dayofweek, week=df['ts'].dt.isocalendar().week, 
     month=df['ts'].dt.month, year=df['ts'].dt.year, weekday=df['ts'].dt.dayofweek > 4)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
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
            songplay_data = (row.ts, int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


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