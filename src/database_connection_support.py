# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes

# data processing
import pandas as pd

# functions typing
from typing import Optional, Tuple, List, Union, Dict

def connect_to_database(database: str, credentials_dict: Dict[str, str], autocommit: bool = False) -> Optional[psycopg2.extensions.connection]:
    """
    Connects to a PostgreSQL database using provided credentials.

    Parameters:
    ----------
    database : str
        Name of the database to connect to.
    credentials_dict : dict
        Dictionary containing 'username' and 'password' for authentication.

    Returns:
    -------
    Optional[psycopg2.extensions.connection]
        A PostgreSQL database connection if successful, None otherwise.
    """
    try:
        connection = psycopg2.connect(
            database=database,
            user=credentials_dict["username"],
            password=credentials_dict["password"],
            host="localhost",
            port="5432"
        )
        connection.autocommit = autocommit
        return connection
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("Invalid password.")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Connection error.")
        else:
            print(f"Error occurred: {e}", e.pgcode)
        return None

def connect_and_query(database: str, credentials_dict: Dict[str, str], query: str, columns: Union[str, list] = "query") -> pd.DataFrame:
    """
    Connects to a database, executes a query, and returns the results as a DataFrame.

    Parameters:
    ----------
    database : str
        Name of the database to connect to.
    credentials_dict : dict
        Dictionary containing 'username' and 'password' for authentication.
    query : str
        SQL query to execute.
    columns : Union[str, list], optional
        Column names for the DataFrame. If 'query', uses columns from the query result.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing the query results.
    """
    connection = connect_to_database(database=database, credentials_dict=credentials_dict)
    
    if not connection:
        return pd.DataFrame()  # Return an empty DataFrame if connection fails
    
    cursor = connection.cursor()
    cursor.execute(query)
    
    if columns == "query":
        columns = [desc[0] for desc in cursor.description]
    elif not isinstance(columns, list):
        columns = None
    
    result_df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    cursor.close()
    connection.close()
    
    return result_df

def alter_update_query(database: str, credentials_dict: Dict[str, str], alter_update_query: str) -> None:
    """
    Connects to a database and executes an ALTER or UPDATE query.

    Parameters:
    ----------
    database : str
        Name of the database to connect to.
    credentials_dict : dict
        Dictionary containing 'username' and 'password' for authentication.
    alter_update_query : str
        SQL query for ALTER or UPDATE operations.
    """
    connection = connect_to_database(database=database, credentials_dict=credentials_dict)
    
    if not connection:
        return  # If connection fails, exit function
    
    cursor = connection.cursor()
    
    with cursor:
        cursor.execute(alter_update_query)
        connection.commit()
    
    cursor.close()
    connection.close()
