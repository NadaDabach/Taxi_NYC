import json

import snowflake.connector as sf
from snowflake import snowflake_config
from config import settings
from snowflake.snowflake_config import execute_query
from snowflake.snowflake_config import create_format_file
from snowflake import taxi_trips
from snowflake import payment
from snowflake import point_of_interest
from snowflake import rate
from snowflake import storeAndFwd
from snowflake import trip
from snowflake import vendor
from snowflake import taxi_zones
import json

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # snowflake_config.create_objects("Taxi_NYC", "Taxi_NYC_warehouse", "Taxi_NYC_schema")
    conn = snowflake_config.snowflake_connect()
    # conn = sf.connect(user='nadimab', password='Johnsnowbimbaz@1', account='VFUZTQL-II47900')
    try:
        sql = 'use {}'.format(settings.database)
        execute_query(conn, sql)

        sql = 'use warehouse {}'.format(settings.warehouse)
        execute_query(conn, sql)

        sql = 'use schema {}'.format(settings.schema)
        execute_query(conn, sql)

        create_format_file(conn)

        """# Create and load TAXI TRIPS
        taxi_trips.create_table_taxi_trips(conn)
        taxi_trips.load_table_taxi_trips(conn)

        # Create and load PAYMENT TABLE
        payment.create_table_payment_type(conn)

        # Create and load RATE table
        rate.create_table_rate_types(conn)

        # Create and load STORE AND FORWARD table
        storeAndFwd.create_table_store_and_fwd(conn)

        # Create and load TRIP table
        trip.create_table_trip(conn)

        # Create and load VENDOR table
        vendor.create_table_vendor(conn)

        # Create and load POINTS OF INTEREST table
        point_of_interest.create_table_point_of_interest(conn)
        point_of_interest.load_table_points_of_interest(conn)"""

        # Create and load TAXI ZONES table
        taxi_zones.create_table_zones_taxi(conn)
        taxi_zones.load_table_zones_taxi(conn)

    except Exception as e:
        print(e)

    finally:
        conn.close
        print('connection disconnected')
