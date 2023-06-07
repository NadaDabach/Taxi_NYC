from snowflake.snowflake_config import execute_query
from config import settings


def create_table_payment_type(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE payment (' \
              'payment_id INT, ' \
              'payment_type VARCHAR)'

        execute_query(conn, sql)

        insert_sql = f"INSERT INTO {settings.database}.{settings.schema}.payment VALUES " \
                     f"(1, 'Credit card'),"\
                     f"(2, 'Cash')," \
                     f"(3, 'No charge')," \
                     f"(4, 'Dispute')," \
                     f"(5, 'Unknown')," \
                     f"(6, 'Voided trip')"
        execute_query(conn, insert_sql)
        print("Loaded data into PAYMENT table")

    except Exception as e:
        print(e)

    finally:
        conn.close
