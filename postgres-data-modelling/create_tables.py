import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import configparser

def create_database(host, username, password, defaultdb, sparkifydb):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=" + host + " dbname=" + defaultdb + " user=" + username + " password=" + password)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS " + sparkifydb)
    cur.execute("CREATE DATABASE " + sparkifydb + " WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=" + host + " dbname=" + sparkifydb + " user=" + username + " password=" + password)
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    # Loading Configuration from ConfigParser
    config = configparser.ConfigParser()
    config.read_file(open('postgres.cfg'))
    HOST=config.get("POSTGRES","HOST")
    USER=config.get("POSTGRES","USER")
    PASSWORD=config.get("POSTGRES","PASSWORD")
    DEFAULT_DB=config.get("POSTGRES","DEFAULT_DB")
    SPARKIFY_DB=config.get("POSTGRES","SPARKIFY_DB")

    cur, conn = create_database(HOST, USER, PASSWORD, DEFAULT_DB, SPARKIFY_DB)
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()