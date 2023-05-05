select cast(objectid as double) as objectid,
 cast(shape_leng as double) as shape_leng,
 the_geom, 
 cast(shape_area as double) as shape_area, 
 zone, 
 cast(locationid as double) as locationid, 
 borough
 from {{ source('taxi_source', 'taxi_zones') }}
 -- drop the rows where length is less than 3 and bigger than 16
 where 
 length(objectid) < 3 and length(shape_leng) <16 and
 shape_area is not NULL and
 zone is not NULL and
 locationid is not NULL and
 borough is not NULL