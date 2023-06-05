select l.taxi_pk, s.payment_description 
from {{source('BV_source','link_b_payment_taxi')}} as l join {{source('BV_source','sat_b_payment')}} as s 
on l.paymenttype_pk = s.paymenttype_pk