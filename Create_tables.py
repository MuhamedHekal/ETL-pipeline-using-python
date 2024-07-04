From Sql_Queries import drop_tables_queries, create_tables_queries
import psycopg2
def Create_db():
''' This Function connect to postgres sql and crate the Data warehouse 
    It return database connection and the cursor 
'''
    try:
        conn = psycopg2.connect('host=127.0.0.1 dbname=postgres user=postgres password=123')
        cur = conn.cursor()
        conn.set_session(autocommit= 'True')
    except psycopg2.Error as e:
        print('cannot connect tho the database')
        print(e)
    try:
        cur.execute('Drop database if exists sparkify')
        cur.execute('Create database sparkify')
    except psycopg2.Error as e:
        print(e)
    
    try:
        conn = psycopg2.connect('host=127.0.0.1 dbname=sparkify user=postgres password=123')
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('cannot connect tho the database Sparkify')
        print(e)
    
    return conn, cur

def Drop_tables(conn, cur):
''' This Function drop all tables in the datawarehouse to reset it 
    Take the conn and the cursor for the database and Drop if the tables exists
    
'''
    for query in drop_tables_queries:
        cur.execute(query)
        conn.commit()
def Create_tables(conn, cur):
''' This Function Create all tables 
     Take the conn and the cursor for the database and Create tables if the tables Not exists
'''
    for query in create_tables_queries:
        cur.execute(query)
        conn.commit()
def main():
'''
    Create the database If Not Exists 
    Drop all tables if Exists 
    Create tables in Not exists

'''
    conn, cur = Create_db()
    Drop_tables(conn, cur)
    Create_table(conn, cur)
    cur.close()
    conn.close()
if __name__ == __main__
    main()