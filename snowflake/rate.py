from snowflake.snowflake_config import execute_query
from config import settings


def create_table_rate_types(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE rate (' \
              'rate_id INT, ' \
              'rate_type VARCHAR)'

        execute_query(conn, sql)

        insert_sql = f"INSERT INTO {settings.database}.{settings.schema}.rate VALUES " \
                     f"(1, 'Standard rate')," \
                     f"(2, 'JFK')," \
                     f"(3, 'Newark')," \
                     f"(4, 'Nassau or Westchester')," \
                     f"(5, 'Negotiated fare')," \
                     f"(6, 'Group ride')"
        execute_query(conn, insert_sql)
        print("Loaded data into RATE table")

    except Exception as e:
        print(e)

    finally:
        conn.close
