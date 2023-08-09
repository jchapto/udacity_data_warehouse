import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops each table using the queries in `drop_table_queries` list.

    INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2

    OUTPUT:
    Drops each table using drop statements from sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates each table using the queries in `create_table_queries` list. 

    INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2

    OUTPUT:
    Creates each table using create statements from sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the redshift database. 
    
    - Establishes connection with the redshift database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to Database...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping existing tables...')
    drop_tables(cur, conn)

    print('Creating new tables...')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()