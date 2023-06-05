select paymenttype_pk,payment_description,total_taxi
from {{source('BV_source','sat_b_payment')}}