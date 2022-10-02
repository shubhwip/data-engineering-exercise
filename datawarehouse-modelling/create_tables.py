import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    datawarehouse_config = configparser.ConfigParser()
    datawarehouse_config.read('dwh.cfg')
    conn_string = "host={} dbname={} user={} password={} port={}".format(*datawarehouse_config['CLUSTER'].values())
    print(conn_string)
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()