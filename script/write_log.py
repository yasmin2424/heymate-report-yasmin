from datetime import datetime, timezone
def write_log(start_row_index, end_row_index, source, status):
    query = f"INSERT INTO TABLE VALUES ({start_row_index}, {end_row_index}, {source}, {status}, {datetime.now(timezone.utc)})"
    return query