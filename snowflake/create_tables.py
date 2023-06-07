import snowflake.connector as sf
from snowflake.snowflake_config import execute_query
import snowflake.connector


def create_tables(conn, table_names):
    cur = conn.cursor()
    for table in table_names:
        cur.execute(f'CREATE TABLE IF NOT EXISTS trajets_taxi ('
                    f'idTrajet INT,'
                    f'vendorID INT,'
                    f'lpepPickupDatetime TIMESTAMP,'
                    f'lpepDropoffDatetime TIMESTAMP,'
                    f'passengerCount INT,'
                    f'tripDistance DOUBLE,'
                    f'puLocationId INT,'
                    f'doLocationId INT,'
                    f'pickupLongitude DOUBLE,'
                    f'pickupLatitude DOUBLE,'
                    f'dropoffLongitude DOUBLE,'
                    f'dropoffLatitude DOUBLE,'
                    f'rateCodeID INT,'
                    f'storeAndFwdFlag VARCHAR,'
                    f'paymentType INT,'
                    f'fareAmount DOUBLE,'
                    f'extra DOUBLE,'
                    f'mtaTax DOUBLE,'
                    f'improvementSurcharge DOUBLE,'
                    f'tipAmount DOUBLE,'
                    f'tollsAmount DOUBLE,'
                    f'ehailFee DOUBLE,'
                    f'totalAmount DOUBLE,'
                    f'totalAmount DOUBLE,);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS zones_taxi ('
                    f'OBJECTID VARCHAR,'
                    f'Shape_Leng VARCHAR,'
                    f'the_geom VARCHAR,'
                    f'Shape_Area VARCHAR,'
                    f'zone VARCHAR,'
                    f'LocationID VARCHAR,'
                    f'borough VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS point_of_interest_taxi ('
                    f'the_geom VARCHAR,'
                    f'SEGMENTID VARCHAR,'
                    f'COMPLEXID INT,'
                    f'SAFTYPE VARCHAR,'
                    f'SOS DOUBLE,'
                    f'PLACEID INT,'
                    f'FACI_DOM DOUBLE,'
                    f'BIN DOUBLE,'
                    f'BOROUGH DOUBLE,'
                    f'CREATED TIMESTAMP,'
                    f'MODIFIED TIMESTAMP,'
                    f'FACILITY_T DOUBLE,'
                    f'SOURCE VARCHAR,'
                    f'B7SC DOUBLE,'
                    f'PRI_ADD DOUBLE,'
                    f'NAME VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS payment ('
                    f'payment_id INT,'
                    f'payment_type VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS tarification ('
                    f'tarification_id INT,'
                    f'tarification_type VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS vendor ('
                    f'vendor_id INT,'
                    f'description VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS trip ('
                    f'trip_id INT,'
                    f'trip_type VARCHAR);')

        cur.execute(f'CREATE TABLE IF NOT EXISTS enregistrement_trajet ('
                    f'enrg_trajet_id VARCHAR,'
                    f'stockage_transmission VARCHAR);')
