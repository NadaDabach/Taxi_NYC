select zones_pk, the_geom, num_points, xmax,xmin,ymax,ymin, wkt, wkb, geojson
from {{ source('BV_source','sat_b_zones') }}