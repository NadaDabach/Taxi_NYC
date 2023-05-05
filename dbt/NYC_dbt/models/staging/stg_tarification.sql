select *
from {{ source('taxi_source', 'tarification') }}