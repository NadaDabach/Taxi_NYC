{# select cast(objectid as double) as objectid,
 cast(shape_leng as double) as shape_leng,
 the_geom, 
 cast(shape_area as double) as shape_area, 
 zone as zone_name, 
 cast(locationid as double) as locationid, 
 borough,
 current_date() as load_date
 from {{ source('taxi_source', 'taxi_zones') }}
 where 
 length(objectid) < 5 and length(shape_leng) <16 and
 shape_area is not NULL and
 zone_name is not NULL and
 locationid is not NULL and
 borough is not NULL #}
 with zon as (
 select objectid,
    shape_leng,
    replace (the_geom, ';',',') as the_geom,
    shape_area,
    zone,
    locationid,
    borough,
    current_date() as load_date
 from {{ source('taxi_source_transformed', 'zones') }} )

select objectid,
    shape_leng,
    to_geometry(the_geom) as the_geom,
    shape_area,
    zone,
    locationid,
    borough,
    load_date from zon