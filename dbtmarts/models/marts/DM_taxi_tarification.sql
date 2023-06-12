select l.taxi_pk, s.tarification_description 
from {{source('BV_source','link_b_tarification_taxi')}} as l join {{source('BV_source','sat_b_tarification')}} as s 
on l.tarificationcode_pk = s.tarificationcode_pk