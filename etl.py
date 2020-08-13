import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    - Loads the data from the s3 json files into staging tables in the redshift cluster
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_to_tables(cur, conn):
    """
    - inserts transformed data from the staging table into the dimension and fact tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()    

def main():
    """
    - Reads in the connection details from the config file
    
    - Establishes connection with the sparkify database and gets
    cursor to it
    
    - calls function to load staging tables
    
    - calls function to insert to the dimension and fact tables
    
    - closes connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_to_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()