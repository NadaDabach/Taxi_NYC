select ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as taxi_id,
vendorID,vendor_description, current_date() as load_date
from  {{ source('taxi_source', 'taxi_vert_nyc') }} AS t join {{ source('taxi_source', 'vendor') }} as v
on t.vendorid = v.vendor_id
