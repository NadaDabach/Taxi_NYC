select *,
CURRENT_TIMESTAMP() as LDTS,
'STATIC REFERENCE DARA' as RSCR
from {{ source('taxi_source', 'payment') }}