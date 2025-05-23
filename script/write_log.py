from sql_reader import connect_to_sql_server
from sql_uploader import create_table_if_not_exists
from datetime import datetime, timezone

def write_log(start_row_index, end_row_index, source, status, message=None):
    table_name = "Log_mds"
    conn, cursor = connect_to_sql_server()

    # Define schema
    schema_sql = f"""
    CREATE TABLE {table_name} (
        start_row INT,
        end_row INT,
        source NVARCHAR(50),
        status NVARCHAR(20),
        message NVARCHAR(1000),
        log_time DATETIME
    )
    """

    try:
        create_table_if_not_exists(cursor, conn, table_name, schema_sql)
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            f"""
            INSERT INTO {table_name} (start_row, end_row, source, status, message, log_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (start_row_index, end_row_index, source, status, message, timestamp)
        )
        conn.commit()
        print(f"Log written: {status} ({start_row_index}-{end_row_index})")
    except Exception as e:
        print(f"Failed to write log: {e}")
        conn.rollback()
    finally:
        conn.close()
