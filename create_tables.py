"""
This script is responsible for creating the necessary tables in the Data warehouse
"""


import configparser
from typing import Any
import psycopg2
from sql_queries import drop_table_queries, create_table_queries

def connect_redshift(DB_HOST: str,
                     DB_NAME: str,
                     DB_USER: str,
                     DB_PASSWORD:str,
                     DB_PORT: str) -> Any:
    """
    This function takes in the necessary parameters and sets up a connection to a DB
    Params:
        DB_HOST: 
        DB_NAME: Name of Database
        DB_USER: Username
        DB_PASSWORD: Password
        DB_PORT: Port of DB
    Output:
        connection: connection object
    """
    try:
        conn = psycopg2.connect(host = DB_HOST,
                                dbname = DB_NAME,
                                user = DB_USER,
                                password = DB_PASSWORD,
                                port = DB_PORT)
    except Exception as e:
        print(e)
    
    return conn

def drop_tables(curr: Any,
                conn: Any) -> Any:
    """
    This function drops tables listed in drop_table_queries
    Params:
        curr: cursor to connection
        conn: connection to db
    Output:
        None
    """
    for drop_query in drop_table_queries:
        try:
            curr.execute(drop_query)
            conn.commit()
        except Exception as e:
            print(e)

def create_tables(curr: Any,
                  conn: Any) -> Any:
    """
    This function creates tables listed in the create_table_queries list
    Params:
        curr: cursor to connection
        conn: connection to db
    Output:
        None
    """
    for create_query in create_table_queries:
        try:
            curr.execute(create_query)
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
    
    print("...Droping tables if already exists...")
    drop_tables(conn=conn, curr=curr)

    print("...Creating the required tables...")
    create_tables(conn=conn, curr=curr)


    # close the connection
    curr.close()
    conn.close()


if __name__ == '__main__':
    main()
