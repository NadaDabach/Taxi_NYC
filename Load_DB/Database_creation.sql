--Creating table taxi_vert_NYC
create or replace table taxi_nyc_database.Nadim_SCHEMA.taxi_vert_NYC
(
    ID double,
    vendorID double,
    lpepPickupDatetime datetime,
    lpepDropoffDatetime datetime,
    passengerCount double,
    tripDistance double,
    puLocationId double,
    doLocationId double,
    pickupLongitude double,
    pickupLatitude double,
    dropoffLongitude double,
    dropoffLatitude double,
    rateCodeID double,
    storeAndFwdFlag varchar,
    paymentType double,
    fareAmount double,
    extra double,
    mtaTax double,
    improvementSurcharge double,
    tipAmount double,
    tollsAmount double,
    ehailFee double,
    totalAmount double,
    tripType double
);

--Creating a file format object for my CSV Data
CREATE OR REPLACE FILE FORMAT mycsvformat
  TYPE = 'CSV'
    error_on_column_count_mismatch=false
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- Create a stage for CSV Data files
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat;

-- Staging the CSV sample Data files in "snowsql"
PUT file://C:\Users\NadimAbouChrouch\Downloads\Datasets\Trajetstaxi\2017\2017\Taxi_vert_NYC_2017_*.csv @my_csv_stage AUTO_COMPRESS=TRUE;

--List the staged file
LIST @my_csv_stage;

-- Copy data into the target table
COPY INTO taxi_vert_NYC
  FROM @my_csv_stage/Taxi_vert_NYC_2017_12.csv.gz
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  ON_ERROR = 'skip_file';

select * from taxi_vert_nyc;

--Remove the copied data files from the stage
REMOVE @my_csv_stage PATTERN='.*.csv.gz';

-- Creating table 
create or replace table taxi_nyc_database.Nadim_SCHEMA.point_of_interest (
the_geom geometry,
SEGMENTID double,
COMPLEXID double,
SAFTYPE varchar,
SOS double,
PLACEID double,
FACI_DOM double,
BIN double,
BOROUGH double,
CREATED datetime,
MODIFIED datetime,
FACILITY_T double,
SOURCE varchar,
B7SC double,
PRI_ADD double,
NAME varchar
);

-- Copy data into the target table (Error)
COPY INTO point_of_interest
  FROM @my_csv_stage/Point_Of_Interest.csv.gz
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  ON_ERROR = 'skip_file';
    
-- Create the taxi zones table
create or replace table taxi_nyc_database.Nadim_SCHEMA.taxi_zones (
OBJECTID varchar,
Shape_Leng varchar,
the_geom varchar,
Shape_area varchar,
zone varchar,
LocationID varchar,
borough varchar
);


create or replace table TAXI_NYC_DATABASE.NADIM_SCHEMA.PAYMENT (
payment_id double,
payment_description varchar
);
--insert into payment
INSERT INTO TAXI_NYC_DATABASE.NADIM_SCHEMA.PAYMENT (PAYMENT_ID, PAYMENT_DESCRIPTION)
VALUES 
(1, 'Carte de crédit'),
(2, 'Argent liquide'),
(3, 'Aucuns frais'),
(4, 'Litige'),
(5, 'Inconnu'),
(6, 'Voyage annulé');

create or replace table TAXI_NYC_DATABASE.NADIM_SCHEMA.TARIFICATION (
rate_id double,
tarification_description varchar
);

INSERT INTO TAXI_NYC_DATABASE.NADIM_SCHEMA.TARIFICATION (RATE_ID, TARIFICATION_DESCRIPTION)
VALUES 
(1, 'Taux standard'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau ou Westchester'),
(5, 'Prix négocié'),
(6, 'Voyage en groupe');


create or replace table TAXI_NYC_DATABASE.NADIM_SCHEMA.VENDOR (
VENDOR_ID double,
VENDOR_DESCRIPTION varchar
);

INSERT INTO TAXI_NYC_DATABASE.NADIM_SCHEMA.VENDOR (VENDOR_ID, VENDOR_DESCRIPTION)
VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc');

create or replace table TAXI_NYC_DATABASE.NADIM_SCHEMA.TRIP (
TRIP_ID double,
TRIP_DESCRIPTION varchar
);

INSERT INTO TAXI_NYC_DATABASE.NADIM_SCHEMA.TRIP (TRIP_ID, TRIP_DESCRIPTION)
VALUES
(1, 'Héler en rue'),
(2, 'Envoyer');

create or replace table taxi_nyc_database.Nadim_transf.zones (
OBJECTID double,
Shape_Leng double,
the_geom varchar,
Shape_area varchar,
zone varchar,
LocationID double,
borough varchar
);

create or replace table taxi_nyc_database.nadim_schema.stockandtransmission (
    s_id varchar,
    s_description varchar
);
INSERT INTO TAXI_NYC_DATABASE.NADIM_SCHEMA.stockandtransmission (s_id, s_description)
VALUES
('Y', 'Trajet stockage et transmission'),
('N', 'Pas un trajet stockage et transmission');

-- Cleaning taxi zones
select cast(objectid as double) as objectid,
 cast(shape_leng as double) as shape_leng,
 the_geom, 
 cast(shape_area as double) as shape_area, 
 zone as zone_name, 
 cast(locationid as double) as locationid, 
 borough,
 current_date() as load_date
 from TAXI_NYC_DATABASE.NADIM_SCHEMA.TAXI_ZONES
 where 
 length(objectid) < 5 and length(shape_leng) <16 and
 shape_area is not NULL and
 zone_name is not NULL and
 locationid is not NULL and
 borough is not NULL;


 --Snowpipe
CREATE or replace NOTIFICATION INTEGRATION DATA_NYC_EVENT
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://stviseosnowflakedbt.queue.core.windows.net/queuenyc'
AZURE_TENANT_ID = 'd6397071-8e3e-45d2-a2d6-36698acf0fea';

SHOW INTEGRATIONS;

desc NOTIFICATION INTEGRATION DATA_NYC_EVENT;

create or replace stage data_nyc_stage
url = 'azure://nycdatablob.blob.core.windows.net/containernyc/'
credentials = (azure_sas_token=
'?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-05-04T20:37:09Z&st=2023-05-04T12:37:09Z&spr=https&sig=f%2FwqrTMczyIDZfWEP%2FE1CRaCOE44nb77HY4Qpb%2FVDRQ%3D'
);

show stages;

ls @DATA_NYC_STAGE;

create or replace pipe data_nyc_pipe
auto_ingest = true
integration = 'DATA_NYC_EVENT'
as
copy into TAXI_NYC_DATABASE.NADIM_SCHEMA.TAXI_VERT_NYC
from @DATA_NYC_STAGE
file_format = mycsvformat;

ALTER PIPE data_nyc_pipe REFRESH;

select * from TAXI_NYC_DATABASE.NADIM_SCHEMA.TAXI_VERT_NYC;