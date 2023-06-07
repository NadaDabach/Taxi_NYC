from snowflake.snowflake_config import execute_query
from config import settings
import json


def create_table_point_of_interest(conn):
    try:
        sql = 'CREATE OR REPLACE TABLE point_of_interest (' \
              'the_geom VARCHAR, ' \
              'SEGMENTID DOUBLE, ' \
              'COMPLEXID DOUBLE, ' \
              'SAFTYPE VARCHAR,' \
              'SOS VARCHAR, ' \
              'PLACEID DOUBLE, ' \
              'FACI_DOM VARCHAR, ' \
              'BIN DOUBLE,' \
              'BOROUGH VARCHAR, ' \
              'CREATED TIMESTAMP, ' \
              'MODIFIED TIMESTAMP, ' \
              'FACILITY_T INT, ' \
              'SOURCE VARCHAR, ' \
              'B7SC VARCHAR, ' \
              'PRI_ADD DOUBLE, ' \
              'NAME VARCHAR)'

        execute_query(conn, sql)

    except Exception as e:
        print(e)

    finally:
        conn.close


def load_table_points_of_interest(conn):
    file_path = r"C:\Users\NadaDabach\Downloads\Datasets\Datasets\PointsOfInterest"
    file = r"PointsOfInterest.geojson"

    # Moving data to staging area
    create_stage = f'CREATE OR REPLACE STAGE Points_Of_Interest_STAGING file_format = GEOJSON_FORMAT DIRECTORY = (ENABLE = TRUE)'
    execute_query(conn, create_stage)

    put_query = fr"PUT file://{file_path}\{file} @Points_Of_Interest_STAGING AUTO_COMPRESS = TRUE"
    execute_query(conn, put_query)

    # To keep data as JSON (rather than just as text), it needs to be loaded into a column with a datatype of VARIANT, not VARCHAR
    # Create the staging table
    staging_table = "CREATE OR REPLACE TABLE poi_staging (json_data VARIANT)"
    execute_query(conn, staging_table)

    # Stage the GeoJSON file into the staging table
    stage_table = f"COPY INTO poi_staging (json_data) FROM @Points_Of_Interest_STAGING/{file}.gz FILE_FORMAT = (FORMAT_NAME = GEOJSON_FORMAT) ON_ERROR = 'continue'"
    execute_query(conn, stage_table)

    """insert_query = f"INSERT INTO {settings.database}.{settings.schema}.point_of_interest (the_geom, SEGMENTID, COMPLEXID, SAFTYPE, SOS, PLACEID, FACI_DOM, BIN, BOROUGH, CREATED, MODIFIED, FACILITY_T, SOURCE, B7SC, PRI_ADD, NAME)  " \
                   f"SELECT " \
                   f"json_data:features.geometry.coordinates as the_geom," \
                   f"json_data:features.properties.segmentid as SEGMENTID," \
                   f"json_data:features.properties.complexid as COMPLEXID," \
                   f"json_data:features.properties.saftype as SAFTYPE," \
                   f"json_data:features.properties.sos as SOS," \
                   f"json_data:features.properties.placeid as PLACEID," \
                   f"json_data:features.properties.faci_dom as FACI_DOM," \
                   f"json_data:features.properties.bin as BIN," \
                   f"json_data:features.properties.borough as BOROUGH," \
                   f"json_data:features.properties.created as CREATED," \
                   f"json_data:features.properties.modified as MODIFIED," \
                   f"json_data:features.properties.facility_t as FACILITY_T," \
                   f"json_data:features.properties.source as SOURCE," \
                   f"json_data:features.properties.b7_sc as B7SC," \
                   f"json_data:features.properties.pri_add as PRI_ADD," \
                   f"json_data:features.properties.name as NAME " \
                   f"FROM {settings.database}.{settings.schema}.poi_staging, lateral flatten ( input => json_data:features, '' ) "
    execute_query(conn, insert_query)"""

    json_data = open(f'{file_path}/{file}')
    data = json.load(json_data)
    len_file = len(data['features'])
    print(len_file)

    for x in range(len_file):
        insert_query = f"INSERT INTO {settings.database}.{settings.schema}.point_of_interest (the_geom, SEGMENTID, COMPLEXID, SAFTYPE, SOS, PLACEID, FACI_DOM, BIN, BOROUGH, CREATED, MODIFIED, FACILITY_T, SOURCE, B7SC, PRI_ADD, NAME) " \
                       f"SELECT " \
                       f"json_data:features[{x}].geometry.coordinates as the_geom," \
                       f"json_data:features[{x}].properties.segmentid as SEGMENTID," \
                       f"json_data:features[{x}].properties.complexid as COMPLEXID," \
                       f"json_data:features[{x}].properties.saftype as SAFTYPE," \
                       f"json_data:features[{x}].properties.sos as SOS," \
                       f"json_data:features[{x}].properties.placeid as PLACEID," \
                       f"json_data:features[{x}].properties.faci_dom as FACI_DOM," \
                       f"json_data:features[{x}].properties.bin as BIN," \
                       f"json_data:features[{x}].properties.borough as BOROUGH," \
                       f"json_data:features[{x}].properties.created as CREATED," \
                       f"json_data:features[{x}].properties.modified as MODIFIED," \
                       f"json_data:features[{x}].properties.facility_t as FACILITY_T," \
                       f"json_data:features[{x}].properties.source as SOURCE," \
                       f"json_data:features[{x}].properties.b7_sc as B7SC," \
                       f"json_data:features[{x}].properties.pri_add as PRI_ADD," \
                       f"json_data:features[{x}].properties.name as NAME " \
                       f"FROM {settings.database}.{settings.schema}.poi_staging"
        execute_query(conn, insert_query)

    # insert_query = fr"INSERT INTO {settings.database}.{settings.schema}.point_of_interest SELECT JSON_DATA:value:geometry:the_geom::VARCHAR, JSON_DATA:value:SEGMENTID::VARCHAR, JSON_DATA:value:COMPLEXID, JSON_DATA:value:SAFTYPE, JSON_DATA:value:SOS, JSON_DATA:value:PLACEID, JSON_DATA:value:FACI_DOM, JSON_DATA:value:BIN, JSON_DATA:value:BOROUGH, JSON_DATA:value:CREATED, JSON_DATA:value:MODIFIED, JSON_DATA:value:FACILITY_T, JSON_DATA:value:SOURCE, JSON_DATA:value:B7SC, JSON_DATA:value:PRI_ADD, JSON_DATA:value:NAME FROM {settings.database}.{settings.schema}.poi_staging"
    # execute_query(conn, insert_query)

    print("table POINT OF INTEREST is loaded successfully")