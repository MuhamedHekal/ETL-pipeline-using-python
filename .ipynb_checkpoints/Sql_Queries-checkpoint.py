# Drop table Queries for drop all columns if exits to reset the tables before ETL pipeline
songplays_table_drop = 'DROP TABLE IF EXISTS songplays'
users_table_drop = 'DROP TABLE IF EXISTS users'
songs_table_drop = 'DROP TABLE IF EXISTS songs'
artists_table_drop = 'DROP TABLE IF EXISTS artists'
time_table_drop = 'DROP TABLE IF EXISTS time'


# Create Table queries 
songplays_table_create = """ 
                        CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id Serial, 
                                start_time timestamp , 
                                user_id int, 
                                level varchar,
                                song_id varchar , 
                                artist_id varchar, 
                                session_id int,
                                location varchar , 
                                user_agent varchar
                                ) 
                        """

users_table_create = """ 
                        CREATE TABLE IF NOT EXISTS users(
                                user_id int PRIMARY KEY,
                                first_name varchar,
                                last_name varchar, 
                                gender varchar,
                                level varchar
                                ) 
                        """

songs_table_create = """ 
                        CREATE TABLE IF NOT EXISTS songs(
                                song_id varchar PRIMARY KEY,
                                title varchar, 
                                artist_id varchar,
                                year int, 
                                duration float
                                ) 
                        """
time_table_create = """ 
                        CREATE TABLE IF NOT EXISTS time(
                                start_time timestamp PRIMARY KEY, 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday int

                                ) 
                        """
artists_table_create = """ 
                        CREATE TABLE IF NOT EXISTS artists(
                                artist_id varchar PRIMARY KEY,
                                name varchar, 
                                location varchar, 
                                latitude numeric, 
                                longitude numeric
                                ) 
                        """

# Insert Queries for insert the data into Database
songplays_table_insert = """
                            insert into songplays(
                            songplay_id , 
                            start_time, 
                            user_id, 
                            level ,
                            song_id , 
                            artist_id , 
                            session_id,
                            location, 
                            user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                            ON CONFLICT DO NOTHING;
                        """
users_table_insert = """
                            insert into users(
                            user_id ,
                            first_name ,
                            last_name , 
                            gender,
                            level) 
                            values(%s,%s,%s,%s,%s) 
                            ON CONFLICT(user_id) DO UPDATE 
                                                    SET level = EXCLUDED.level;
                     """
songs_table_insert = """
                            insert into songs(
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)values(%s,%s,%s,%s,%s) 
                            ON CONFLICT DO NOTHING;
                     """

artists_table_insert = """
                            insert into artists(
                            artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude)values(%s,%s,%s,%s,%s) 
                            ON CONFLICT DO NOTHING;
                        """
time_table_insert    =  """
                            insert into time(
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year,
                            weekday)values(%s,%s,%s,%s,%s,%s,%s) 
                            ON CONFLICT DO NOTHING;
                        """

songs_select = """
                 SELECT s.song_id, a.artist_id  
                 FROM songs s INNER JOIN artists a 
                 ON s.artist_id = a.artist_id
                 WHERE s.title    =%s  
                 AND   s.duration = %s
                 AND   a.name     = %s
               """


drop_tables_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
create_tables_queries = [songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]