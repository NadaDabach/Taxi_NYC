import snowflake.connector as sf
from snowflake.connector.errors import DatabaseError, ProgrammingError
from config import settings
from log import logger


def create_objects(database_name, warehouse_name, schema_name):
    conn = sf.connect(
        user=settings.user,
        password=settings.password,
        account=settings.account
    )

    # Create database
    conn.cursor().execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    print(f'{database_name} created successfully.')

    # Create warehouse
    conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    print(f'{warehouse_name} created successfully.')

    # Create schema
    conn.cursor().execute(f'CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}')
    print(f'{schema_name} created successfully.')


def snowflake_connect():
    try:
        conn = sf.connect(
            user=settings.user,
            password=settings.password,
            account=settings.account,
            warehouse=settings.warehouse,
            database=settings.database,
            schema=settings.schema
        )
        logger.info("Connection established successfully")
    except DatabaseError as db_ex:
        if db_ex.errno == 250001:
            print(f"Invalid username/password, please re-enter username and password...")
            logger.warning(f"Invalid username/password, please re-enter username and password...")
        else:
            raise
    except Exception as ex:
        print(f"New exception raised {ex}")
        logger.error(f"New exception raised {ex}")
        raise

    cs = conn.cursor()

    print('Connection established')
    cs.execute("SELECT current_version()")
    ver = cs.fetchone()
    print('Snowflake Version is :  ' + str(ver[0]))
    cs.execute("SELECT current_user()")
    usr = cs.fetchone()
    print('Snowflake User is :' + str(usr[0]))

    return conn


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


def create_format_file(conn):
    # Create file format
    csv_format = "CREATE OR REPLACE FILE FORMAT TAXI_NYC TYPE = 'CSV' SKIP_HEADER = 1"
    execute_query(conn, csv_format)

    geojson_format = "CREATE OR REPLACE FILE FORMAT GEOJSON_FORMAT TYPE = 'JSON' COMPRESSION = GZIP STRIP_OUTER_ARRAY = true  IGNORE_UTF8_ERRORS = TRUE"
    execute_query(conn, geojson_format)

