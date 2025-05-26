import os
import pymssql
import pandas as pd
import numpy as np
from sql_reader import connect_to_sql_server

# --------------------------------1. Create table if not exist-----------------------------
def create_table_if_not_exists(cursor, conn, table_name, schema_sql):
    """
    Creates a SQL table if it does not already exist.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Active database cursor for executing the SQL command.
    conn : pymssql.Connection
        Active database connection to commit the transaction.
    table_name : str
        Name of the table to check or create.
    schema_sql : str
        SQL DDL statement to create the table.
    """
    cursor.execute(f"""
    IF OBJECT_ID('{table_name}', 'U') IS NULL
    BEGIN
        {schema_sql}
    END
    """)
    conn.commit()
    print(f"Table {table_name} is ready.")

# --------------------------------2. Truncate table if necessary---------------------------
def truncate_table(cursor, conn, table_name):
    """
    Truncates (clears) all data from the specified SQL table.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Active database cursor for executing the command.
    conn : pymssql.Connection
        Active database connection to commit the transaction.
    table_name : str
        The name of the table to truncate.
    """
    print(f"Truncating table {table_name}...")
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()

# --------------------------------3. Clean the dataframe----------------------------------
def clean_dataframe(df):
    """
    Cleans a pandas DataFrame by standardizing list and null values.

    - Converts list-type entries to comma-separated strings.
    - Converts non-null, non-list entries to strings.
    - Replaces empty or invalid entries with None.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to clean.

    Returns
    -------
    pd.DataFrame
        The cleaned DataFrame.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: 
            ", ".join(x) if isinstance(x, list) and len(x) > 0 else
            str(x) if x is not None and not isinstance(x, list) and not pd.isnull(x) else
            None
        )
    return df

# --------------------------------4. Upload cleaned menu-----------------------------------
def process_and_upload(
    cleaned_results: list[dict],
    source: str, 
    table_name: str = None, 
    truncate: bool = False, 
):
    """
    Upload cleaned menu data to SQL Server.

    This function takes in a list of dictionaries (usually cleaned LLM outputs),
    transforms it into a pandas DataFrame, cleans the data, creates the corresponding
    SQL table if it doesn't exist, and inserts the data in bulk.

    Parameters
    ----------
    cleaned_results : list of dict
        A list where each item is a dictionary with keys:
        - item_id: Unique identifier of the menu item
        - row_id: Row index from original dataset
        - dish_base: Cleaned base name of the dish
        - dish_flavor: Flavors or descriptors (comma-separated string)
        - is_combo: Whether the item is a combo (Yes/No)
        - restaurant_type_std: Standardized restaurant type

    source : str
        Indicates data source. Must be "training" or "testing". 
        Used to determine default table name if not explicitly provided.

    table_name : str, optional
        Custom table name to write to. If not provided, defaults to:
        - "cleaned_menu_mds" for training
        - "cleaned_internal_menu_mds" for testing

    truncate : bool, default False
        If True, clears the existing table before inserting new rows.

    Returns
    -------
    int
        The number of rows successfully uploaded.

    Raises
    ------
    RuntimeError
        If there is any issue during upload (e.g., table schema mismatch, DB error).
    ValueError
        If the source argument is invalid or cleaned data rows are incorrectly formatted.
    """

    # Determine table name if not provided
    if table_name is None:
        if source == "training":
            table_name = "cleaned_menu_mds"
        elif source == "testing":
            table_name = "cleaned_internal_menu_mds"
        else:
            raise ValueError("source must be 'training' or 'testing'")

    # Convert to DataFrame and clean
    df = pd.DataFrame(cleaned_results)
    df = df.replace({np.nan: None, "": None})
    df = clean_dataframe(df)

    # Connect to SQL Server
    conn, cursor = connect_to_sql_server()

    # Define schema
    schema_sql = f"""
    CREATE TABLE {table_name} (
        item_id NVARCHAR(50) PRIMARY KEY,
        row_id NVARCHAR(50),
        dish_base NVARCHAR(255),
        dish_flavor NVARCHAR(255),
        is_combo NVARCHAR(50),
        restaurant_type_std NVARCHAR(255)
    )
    """

    try:
        # Step 1: Create table if not exists
        create_table_if_not_exists(cursor, conn, table_name, schema_sql)

        # Step 2: Truncate table if needed
        if truncate:
            truncate_table(cursor, conn, table_name)

        # Step 3: Prepare cleaned rows
        cleaned = [
            (
                row["item_id"],
                row["row_id"],
                row["dish_base"],
                row["dish_flavor"],
                row["is_combo"],
                row["restaurant_type_std"]
            )
            for _, row in df.iterrows()
        ]

        if not all(len(row) == 6 for row in cleaned):
            raise ValueError("Some rows do not have exactly 5 fields.")

        # Step 4: Upload
        cursor.executemany(
            f"""
            INSERT INTO {table_name} (item_id, row_id, dish_base, dish_flavor, is_combo, restaurant_type_std)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            cleaned
        )
        conn.commit()
        print(f"Uploaded {len(cleaned)} rows to {table_name}")
        return len(cleaned)

    except Exception as e:
        print("Upload failed. Rolling back transaction...")
        conn.rollback()
        raise RuntimeError(f"Upload failed: {e}")

    finally:
        conn.close()