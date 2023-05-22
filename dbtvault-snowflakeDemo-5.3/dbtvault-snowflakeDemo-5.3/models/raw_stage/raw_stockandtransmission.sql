select ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as taxi_id,
storeandfwdflag,s_description, current_date() as load_date
from  {{ source('taxi_source', 'taxi_vert_nyc') }} AS t join {{ source('taxi_source', 'stockandtransmission') }} as s
on (t.storeandfwdflag = s.s_id)