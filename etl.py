"""
This script is responsible for running the etl queries and inserting the data
"""

import psycopg2
import configparser
from typing import Any
from create_tables import connect_redshift
from sql_queries import insert_table_queries, copy_table_queries

def copy_table(curr: Any,
               conn: Any) -> None:
    """

    """
    for query in copy_table_queries:
        try:
            curr.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def insert_table(curr: Any,
                 conn: Any) -> None:
    """
    
    """
    for query in insert_table_queries:
        try:
            curr.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def main():
    config = configparser.ConfigParser()
    config.read('path to file')
    
    print("...Getting connection config from config file...")

    # Redshift connection details
    DWH_HOST = config['CLUSTER']['HOST']
    DWH_NAME = config['CLUSTER']['DB_NAME']
    DWH_USER = config['CLUSTER']['DB_USER']
    DWH_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    DWH_PORT = config['CLUSTER']['DB_PORT']

    # create connection

    print("...Creating connection to Redshift...")

    conn = connect_redshift(DB_HOST=DWH_HOST,
                            DB_NAME=DWH_NAME,
                            DB_USER=DWH_USER,
                            DB_PASSWORD=DWH_PASSWORD,
                            DB_PORT=DWH_PORT)
    
    curr = conn.cursor()

    copy_table(curr=curr, conn=conn)

    insert_table(curr=curr, conn=conn)

    # close the connection
    curr.close()
    conn.close()

if __name__ == '__main__':
    main()