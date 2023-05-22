import snowflake.connector as sf
from snowflake import snowflake_config
from config import settings
from snowflake.snowflake_config import execute_query

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # snowflake_config.create_objects("Taxi_NYC", "Taxi_NYC_warehouse", "Taxi_NYC_schema")
    conn = snowflake_config.snowflake_connect()
    try:
        sql = 'use {}'.format(settings.database)
        execute_query(conn, sql)

        sql = 'use warehouse {}'.format(settings.warehouse)
        execute_query(conn, sql)

        try:
            sql = 'alter warehouse {} resume'.format(settings.warehouse)
            execute_query(conn, sql)
        except:
            pass

    except Exception as e:
        print(e)

    finally:
        conn.close
