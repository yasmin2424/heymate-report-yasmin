import os
import pymssql
import pandas as pd
from dotenv import load_dotenv


# ---------------------------------1. Connect to SQL Server--------------------------------
def connect_to_sql_server():
    """
    Establishes and returns a connection and cursor to the SQL Server using credentials from a .env file.

    Returns
    -------
    tuple
        A tuple containing:
        - conn : pymssql.Connection
            The established SQL Server connection.
        - cursor : pymssql.Cursor
            Cursor object associated with the connection.
    """
    load_dotenv(dotenv_path="../credentials/.env")

    conn = pymssql.connect(
        server=os.getenv("SQL_SERVER"),
        user=os.getenv("SQL_USER"),
        password=os.getenv("SQL_PASSWORD"),
        database=os.getenv("SQL_DATABASE")
    )
    return conn, conn.cursor()

# --------------------------2. Read DataFrame from SQL Server table------------------------
def read_dataframe_from_sql(query, conn):
    """
    Executes a SQL query and returns the result as a pandas DataFrame.

    Parameters
    ----------
    query : str
        The SQL query to execute.
    conn : pymssql.Connection
        The active SQL Server connection.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    Raises
    ------
    RuntimeError
        If query execution fails.
    """  
    try:
        df = pd.read_sql(query, conn)
        print(f"Read {len(df)} rows from SQL Server.")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to query data from SQL Server: {e}")

# --------------------------3. Get data batch from SQL Server -----------------------------
def get_data_batch(start: int, end: int, source: str) -> pd.DataFrame:
    """
    Retrieves a batch of data from SQL Server based on row range and data source type.

    Parameters
    ----------
    start : int
        Start index for row selection.
    end : int
        End index for row selection.
    source : str
        Either 'training' or 'testing', indicating the source table.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the requested batch of data.

    Raises
    ------
    ValueError
        If the source parameter is not 'training' or 'testing'.
    RuntimeError
        If data fetching fails.
    """
    conn, cursor = connect_to_sql_server()

    if source == "training":
        query = f"""
        SELECT m.id AS item_id, m.row_id, m.menu_name AS item_name, m.menu_category, m.menu_item_description, 
               m.restaurant_name, m.restaurant_id, r.restaurant_type
        FROM Menu_mds_sorted m
        JOIN Restaurants_mds r ON m.restaurant_id = r.id
        WHERE row_id BETWEEN {start} AND {end}
        """
    elif source == "testing":
        query = f"""
        SELECT *
        FROM Internal_menu_mds
        WHERE row_id BETWEEN {start} AND {end}
        """
    else:
        raise ValueError("Invalid source. Must be 'training' or 'testing'.")

    df = read_dataframe_from_sql(query, conn)
    conn.close()
    return df
