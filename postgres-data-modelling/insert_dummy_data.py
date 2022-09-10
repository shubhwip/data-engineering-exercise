import psycopg2
from sql_queries import insert_table_queries, insert_table_record
import configparser

def create_database(host, username, password, sparkifydb):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    # connect to sparkify database
    conn = psycopg2.connect("host=" + host + " dbname=" + sparkifydb + " user=" + username + " password=" + password)
    cur = conn.cursor()
    
    return cur, conn


def insert_data(cur, conn):
    """
    Inserts data into each table using queries from `insert_table_queries` list. 
    """
    for query, record in zip(insert_table_queries, insert_table_record):
        print(query)
        print(record)
        cur.execute(query, record)
        conn.commit()

def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Insert dummy data into it
    
    - Finally, closes the connection. 
    """
    # Loading Configuration from ConfigParser
    config = configparser.ConfigParser()
    config.read_file(open('postgres.cfg'))
    HOST=config.get("POSTGRES","HOST")
    USER=config.get("POSTGRES","USER")
    PASSWORD=config.get("POSTGRES","PASSWORD")
    SPARKIFY_DB=config.get("POSTGRES","SPARKIFY_DB")

    cur, conn = create_database(HOST, USER, PASSWORD, SPARKIFY_DB)
    
    insert_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

