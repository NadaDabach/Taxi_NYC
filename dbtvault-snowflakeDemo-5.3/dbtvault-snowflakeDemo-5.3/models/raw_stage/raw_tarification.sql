select ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as taxi_id,
ratecodeid,tarification_description, current_date() as load_date
from  {{ source('taxi_source', 'taxi_vert_nyc') }} AS t join {{ source('taxi_source', 'tarification') }} as p
on t.ratecodeid = p.rate_id
