# python database manager
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# work with environment variables
import os
from dotenv import load_dotenv
load_dotenv()
USERNAME = os.getenv("DATABASE_USERNAME")
PASSWORD = os.getenv("DATABASE_PASSWORD")

# typing
from typing import List, Optional

from .database_connection_support import connect_to_database



def create_db(database_name, credentials_dict):
    # connect to default postgres database
    conn = connect_to_database("postgres", credentials_dict) 

    # Set isolation level to autocommit to avoid transaction block
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    # create cursor and check if database exists
    cur = conn.cursor()
    
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
    
    # store database existance result
    database_exists = cur.fetchone()
    
    # if not exists, create
    if not database_exists:
        cur.execute(f"CREATE DATABASE {database_name};")
        print(f"Database {database_name} created succesfully.")
    else:
        print(f"Database already existant.")
        
    # Close cursor and connection
    cur.close()
    conn.close()

def drop_tables(conn: psycopg2.extensions.connection, lista_tablas: List[str]) -> None:
    """
    Drops all tables from the database with CASCADE.

    Parameters:
    ----------
        - conn (psycopg2.extensions.connection): Connection to the PostgreSQL database.
        - lista_tablas (List[str]): List of tables to drop.
    """
    
    lista_tablas_string = ", ".join(lista_tablas)

    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {lista_tablas_string} CASCADE;"
        )
        conn.commit()