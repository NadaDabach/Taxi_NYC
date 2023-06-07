from snowflake.snowflake_config import execute_query
from config import settings


def create_table_vendor(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE vendor (' \
              'vendor_id INT, ' \
              'vendor_description VARCHAR)'

        execute_query(conn, sql)

        insert_sql = f"INSERT INTO {settings.database}.{settings.schema}.vendor VALUES " \
                     f"(1, 'Creative Mobile Technologies, LLC')," \
                     f"(2, 'VeriFone Inc.')"
        execute_query(conn, insert_sql)
        print("Loaded data into VENDOR table")

    except Exception as e:
        print(e)

    finally:
        conn.close
