select ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as taxi_id,
triptype,trip_description, current_date() as load_date
from  {{ source('taxi_source', 'taxi_vert_nyc') }} AS t join {{ source('taxi_source', 'trip') }} as tr
on t.triptype = tr.trip_id
