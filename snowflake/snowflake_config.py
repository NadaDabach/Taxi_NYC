import snowflake.connector as sf
from config import settings


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
    conn = sf.connect(
        user=settings.user,
        password=settings.password,
        account=settings.account,
        warehouse=settings.warehouse,
        database=settings.database,
        schema=settings.schema
    )

    cs = conn.cursor()

    try:
        print('Connection established')
        cs.execute("SELECT current_version()")
        ver = cs.fetchone()
        print('Snowflake Version is :  ' + str(ver[0]))
        cs.execute("SELECT current_user()")
        usr = cs.fetchone()
        print('Snowflake User is :' + str(usr[0]))
    finally:
        cs.close()
        print('connection disconnected')

    return conn


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
