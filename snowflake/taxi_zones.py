import json
from config import settings
from snowflake.snowflake_config import execute_query


def create_table_zones_taxi(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE taxi_zones (' \
              'OBJECTID DOUBLE, ' \
              'Shape_Leng DOUBLE,' \
              'the_geom VARCHAR, ' \
              'Shape_Area DOUBLE,' \
              'zone VARCHAR,' \
              'LocationID DOUBLE,' \
              'borough VARCHAR)'

        execute_query(conn, sql)

    except Exception as e:
        print(e)

    finally:
        conn.close


def load_table_zones_taxi(conn):
    file_path = rf"C:\Users\NadaDabach\Downloads\Datasets\Datasets\GeolocalisationZonesTaxi"
    file_name = r"NYCTaxiZones.geojson"

    # Moving data to staging area
    create_stage = f'CREATE OR REPLACE STAGE Zones_Taxi_STAGING file_format = GEOJSON_FORMAT DIRECTORY = (ENABLE = TRUE)'
    execute_query(conn, create_stage)

    put_query = fr"PUT file://{file_path}\{file_name} @Zones_Taxi_STAGING AUTO_COMPRESS = TRUE"
    execute_query(conn, put_query)

    # Create the staging table
    staging_table = "CREATE OR REPLACE TABLE zt_staging (zones_data VARIANT)"
    execute_query(conn, staging_table)

    # Stage the GeoJSON file into the staging table
    stage_table = f"COPY INTO zt_staging (zones_data) FROM @Zones_Taxi_STAGING/{file_name}.gz FILE_FORMAT = (FORMAT_NAME = GEOJSON_FORMAT) ON_ERROR = 'continue'"
    execute_query(conn, stage_table)

    zones_data = open(f'{file_path}/{file_name}')
    data = json.load(zones_data)
    len_file = len(data['features'])
    print(len_file)

    for x in range(len_file):
        insert_data = f"INSERT INTO {settings.database}.{settings.schema}.taxi_zones(OBJECTID, Shape_Leng, the_geom, Shape_Area, zone, LocationID, borough) " \
                      f"SELECT " \
                      f"zones_data:features[{x}].properties.objectid as OBJECTID," \
                      f"zones_data:features[{x}].properties.shape_leng as Shape_Leng," \
                      f"zones_data:features[{x}].geometry.coordinates as the_geom," \
                      f"zones_data:features[{x}].properties.shape_area as Shape_Area," \
                      f"zones_data:features[{x}].properties.zone as zone," \
                      f"zones_data:features[{x}].properties.location_id as LocationID," \
                      f"zones_data:features[{x}].properties.borough as borough " \
                      f"FROM {settings.database}.{settings.schema}.zt_staging"
        execute_query(conn, insert_data)

    print("table TAXI ZONES is loaded successfully")
