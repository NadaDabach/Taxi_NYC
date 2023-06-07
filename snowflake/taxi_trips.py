from snowflake.snowflake_config import execute_query
from snowflake.snowflake_config import create_format_file


def create_table_taxi_trips(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE taxi_trips (' \
              'idTrajet INT, ' \
              'vendorID INT, ' \
              'lpepPickupDatetime TIMESTAMP, ' \
              'lpepDropoffDatetime TIMESTAMP,' \
              'passengerCount INT, ' \
              'tripDistance DOUBLE, ' \
              'puLocationId INT, ' \
              'doLocationId INT,' \
              'pickupLongitude DOUBLE, ' \
              'pickupLatitude DOUBLE, ' \
              'dropoffLongitude DOUBLE, ' \
              'dropoffLatitude DOUBLE, ' \
              'rateCodeID INT, ' \
              'storeAndFwdFlag VARCHAR, ' \
              'paymentType INT, ' \
              'fareAmount DOUBLE, ' \
              'extra DOUBLE, ' \
              'mtaTax DOUBLE, ' \
              'improvementSurcharge DOUBLE, ' \
              'tipAmount DOUBLE, ' \
              'tollsAmount DOUBLE, ' \
              'ehailFee DOUBLE, ' \
              'totalAmount DOUBLE,' \
              'tripType DOUBLE)'

        execute_query(conn, sql)

    except Exception as e:
        print(e)

    finally:
        conn.close


def load_table_taxi_trips(conn):
    #files_path = r"C:\Users\NadaDabach\Downloads\Datasets\Datasets\Trajetstaxi\2017"
    files = ["Taxi_vert_NYC_2017_01_2", "Taxi_vert_NYC_2017_01_1", "Taxi_vert_NYC_2017_02_1", "Taxi_vert_NYC_2017_02_2", "Taxi_vert_NYC_2017_03_1", "Taxi_vert_NYC_2017_03_2",
             "Taxi_vert_NYC_2017_04", "Taxi_vert_NYC_2017_05", "Taxi_vert_NYC_2017_06", "Taxi_vert_NYC_2017_07", "Taxi_vert_NYC_2017_08", "Taxi_vert_NYC_2017_09",
             "Taxi_vert_NYC_2017_10", "Taxi_vert_NYC_2017_11", "Taxi_vert_NYC_2017_12"]

    # Moving data to staging area
    create_stage = f'CREATE OR REPLACE STAGE TAXI_TRIPS_STAGING file_format = TAXI_NYC DIRECTORY = (ENABLE = TRUE)'
    execute_query(conn, create_stage)

    put_query = r"PUT file://C:\Users\NadaDabach\Downloads\Datasets\Datasets\Trajetstaxi\2017\*.csv @TAXI_TRIPS_STAGING AUTO_COMPRESS = TRUE"
    execute_query(conn, put_query)

    list_query = 'LIST @TAXI_TRIPS_STAGING'
    print(execute_query(conn, list_query))

    # Moving data from staging to source table with all correct data types
    for file_paths in files:
        copy_sql = f"COPY INTO taxi_trips FROM @TAXI_TRIPS_STAGING/{file_paths}.csv.gz FILE_FORMAT = (FORMAT_NAME = TAXI_NYC) ON_ERROR = 'continue'"
        execute_query(conn, copy_sql)
        print(f'Loaded data from {file_paths} into taxi_trips table.')

    # copy_query = "COPY INTO taxi_trips FROM @TAXI_TRIPS_STAGING/*.csv.gz FILE_FORMAT = (FORMAT_NAME = TAXI_NYC) ON_ERROR = 'continue'"
    # execute_query(conn, copy_query)
