select
ZONES_PK,
HASHDIFF,
SHAPE_LENG,
THE_GEOM,
ST_NPoints(THE_GEOM) as num_points,
ST_AsText(THE_GEOM) as wkt,
ST_XMAX(THE_GEOM) as xmax,
ST_XMIN(THE_GEOM) as xmin,
ST_YMAX(THE_GEOM) as ymax,
ST_YMIN(THE_GEOM) as ymin,
SHAPE_AREA,
ZONE,
BOROUGH,
EFFECTIVE_FROM,
LOAD_DATE,
RECORD_SOURCE
from {{ ref('sat_zones')}}
