import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

#runs the queries that will laod the data into the staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        

#runs the queries that will laod the data into the star schema tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
        
#creat the database concection to the redshift database, and create the cursor and call the
# the loading load_staging_tables and the insert_tables function
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()