from snowflake.snowflake_config import execute_query
from config import settings


def create_table_store_and_fwd(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE store_and_fwd (' \
              'trip_record_id VARCHAR, ' \
              'trip_record_description VARCHAR)'
        execute_query(conn, sql)

        insert_sql = f"INSERT INTO {settings.database}.{settings.schema}.store_and_fwd VALUES " \
                     f"('Y', 'Store and forward trip')," \
                     f"('N', 'Not a tore and forward trip')"
        execute_query(conn, insert_sql)
        print("Loaded data into VENDOR table")

    except Exception as e:
        print(e)

    finally:
        conn.close
