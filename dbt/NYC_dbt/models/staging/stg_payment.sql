select *
from {{ source('taxi_source', 'payment') }}