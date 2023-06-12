select tarificationcode_pk,tarification_description,total_taxi
from {{source('BV_source','sat_b_tarification')}}