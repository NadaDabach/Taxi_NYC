select *, current_date() as load_date
from {{ source('taxi_source', 'point_of_interest') }}