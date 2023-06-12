select taxi_pk, fareamount, extra, mtatax, improvementsurcharge, tipamount, tollsamount, totalamount
from {{ source('BV_source', 'sat_b_taxi') }}