from Sql_Queries import * 
import psycopg2
import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')

def get_json_file_paths(folder_path):
    """
    Recursively retrieves the absolute paths of all JSON files in the specified folder and its subfolders.
    Args:
        folder_path (str): Path to the root folder containing JSON files.
    Returns:
        list: A list of absolute file paths.
    """
    json_file_paths = []
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith(".json"):
                json_file_paths.append(os.path.join(root, filename))
    return json_file_paths


def processing_songs_data(cur, file_path):
    '''
    This function take a songs data file path and insert the specific data into songs and artists table in sparkify database
    '''

    # read the dataset from the path
    df = pd.read_json(file_path, lines=True)
    # insert the data into songs table
    songs = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(songs_table_insert,songs)

    # insert the data into artists table
    artists= df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artists_table_insert, artists)
    

def processing_logs_data(cur, file_path):
    '''
    This fucion take a logs data file path and insert the specific data into users, time and songplays table in the sparkify database
    '''
    df = pd.read_json(file_path, lines=True)
    df = df[df['page'] == 'NextSong']
    # convert ts in dataframe to datatime data type
    df['ts'] = pd.to_datetime( df['ts'], unit='ms')
    # loop for each row and insert the data into users and time table
    for i, row in df.iterrows():
        # insert to users table
        cur.execute(users_table_insert, [row.userId, row.firstName, row.lastName, row.gender, row.level ])
    # prepare time dataframe to insert it into time table
    time = df['ts']
    time_df = pd.DataFrame(columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_df['start_time'] = time
    time_df['hour'] = time.dt.hour
    time_df['day'] = time.dt.day
    time_df['week'] = time.dt.isocalendar().week
    time_df['month'] = time.dt.month
    time_df['year'] = time.dt.year
    time_df['weekday'] = time.dt.weekday
    # insert time data into time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)


    # Process songplays data

    for i , row in df.iterrows():
        cur.execute(songs_select,[row.song,row.length, row.artist ])
        results = cur.fetchone()
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
        songplays = [i,row.ts, row.userId,row.level,song_id, artist_id,row.sessionId, row.location, row.userAgent]
        cur.execute(songplays_table_insert, songplays)


def processing_data(cur,conn, folder_path, func):
    '''
    This fuction take tha main folder that contain data files and return all files in specific folder even if songs_data or logs_data
    then send the data for its processing fuction either processing_songs_data or processing_logs_data to insert the data into tables 
    '''
    pathes = get_json_file_paths(folder_path)
    for i, path in enumerate(pathes,1):
        func(cur,path)
        conn.commit()
        print('{}/{} Files Process in {} folder'.format(i,len(pathes), folder_path.split('/')[-1]),end='\r')
    print('')
        
    

def main():
    '''
    makes connection the the sparkify databse build the cusrsor and processing the data 
    '''
    conn = psycopg2.connect('host= 127.0.0.1 dbname=sparkify user=postgres password=123')
    cur= conn.cursor()
    processing_data(cur,conn, './data/song_data' , processing_songs_data)
    processing_data(cur,conn, './data/log_data' , processing_logs_data)
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()