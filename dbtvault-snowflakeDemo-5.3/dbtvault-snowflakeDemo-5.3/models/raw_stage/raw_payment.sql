select ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as taxi_id,
paymenttype,payment_description, current_date() as load_date
from  {{ source('taxi_source', 'taxi_vert_nyc') }} AS t join {{ source('taxi_source', 'payment') }} as p
on t.paymenttype = p.payment_id
