select cast(objectid as double) as objectid,
 cast(shape_leng as double) as shape_leng,
 the_geom, 
 cast(shape_area as double) as shape_area, 
 zone, 
 cast(locationid as double) as locationid, 
 borough
 from 
 taxi_nyc_database.nadim_schema.taxi_zones
 -- drop the rows whene length is less than 3 and bigger than 16
 where 
 length(objectid) < 3 and length(shape_leng) <16