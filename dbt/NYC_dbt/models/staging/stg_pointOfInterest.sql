select *
from {{ source('taxi_source', 'point_of_interest') }}