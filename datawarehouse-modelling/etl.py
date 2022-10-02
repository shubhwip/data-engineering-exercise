import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load staging tables creates staging tables with copy command
    it populates the staging tables from songs and logs data present in s3 storage
    """
    print("Loading staging tables")
    for query in copy_table_queries:
        print("executing load staging table query " + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert tables function executes all insert queries
    it inserts data from staging tables
    """
    print("Inserting data from staging tables to dimension tables")
    for query in insert_table_queries:
        print("executing insert table query " + query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()