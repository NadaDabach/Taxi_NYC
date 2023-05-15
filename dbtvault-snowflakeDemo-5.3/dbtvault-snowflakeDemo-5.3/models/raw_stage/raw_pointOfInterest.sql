{# with poi as (
    select 
    replace(the_geom, 'POINT (', '') as the_geom_1,
    segmentid,
    complexid,
    saftype,
    sos,
    placeid,
    faci_dom,
    bin,
    borough,
    created,
    modified,
    facility_t,
    source,
    b7sc,
    pri_add,
    name,
    current_date() as load_date  from {{ source('taxi_source', 'point_of_interest') }}
),
    pio as (
        select substring(the_geom_1,1,length(the_geom_1)-1) as PIO_geom,
        segmentid,
        complexid,
        saftype,
        sos,
        placeid,
        faci_dom,
        bin,
        borough as borough_poi,
        created,
        modified,
        facility_t,
        source,
        b7sc,
        pri_add,
        name,
        load_date
        from poi
    ), 
    zones_1 as (
        select 
        replace(the_geom, 'MULTIPOLYGON (((', '') as the_geom_1,
        objectid
        from {{ source('taxi_source_transformed', 'zones') }}
    ),
    zones_2 as (
        select substring(the_geom_1,1,length(the_geom_1)-3) as the_geom,
        objectid
        from zones_1
    )

select 
    PIO_geom,
    segmentid,
    complexid,
    saftype,
    sos,
    placeid,
    faci_dom,
    bin,
    borough_poi,
    created,
    modified,
    facility_t,
    source,
    b7sc,
    pri_add,
    name,
    load_date,
    objectid,
    the_geom
from pio as p left join zones_2 as z 
on p.PIO_geom like concat('%',z.the_geom,'%') #}

 with zon as (
    select objectid,
        replace (the_geom, ';',',') as the_geom
    from {{ source('taxi_source_transformed', 'zones') }} ),

    zon_geom as (
        select objectid,
        to_geometry(the_geom) as the_geom
        from zon
    ),
    poi as (
        select 
        the_geom,
        segmentid,
        complexid,
        saftype,
        sos,
        placeid,
        faci_dom,
        bin,
        borough,
        created,
        modified,
        facility_t,
        source,
        b7sc,
        pri_add,
        name 
        from {{ source('taxi_source', 'point_of_interest') }}
    )

    select 
        p.the_geom as poi_geom,
        segmentid,
        complexid,
        saftype,
        sos,
        placeid,
        faci_dom,
        bin,
        p.borough,
        created,
        modified,
        facility_t,
        source,
        b7sc,
        pri_add,
        name,
        current_date() as load_date,
        objectid,
        z.the_geom 
    from poi as p left join zon_geom as z
    on ST_Contains(z.the_geom, p.the_geom)