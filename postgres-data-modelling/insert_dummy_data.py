import psycopg2
from sql_queries import insert_table_queries, insert_table_record

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=shubhamjain password=")
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
    cur, conn = create_database()
    
    insert_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

