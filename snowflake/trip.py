from snowflake.snowflake_config import execute_query
from config import settings


def create_table_trip(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE trip (' \
              'trip_id INT, ' \
              'vendor_type VARCHAR)'

        execute_query(conn, sql)

        insert_sql = f"INSERT INTO {settings.database}.{settings.schema}.trip VALUES " \
                     f"(1, 'Street-hail')," \
                     f"(2, 'Dispatch')"
        execute_query(conn, insert_sql)
        print("Loaded data into TRIP table")

    except Exception as e:
        print(e)

    finally:
        conn.close
