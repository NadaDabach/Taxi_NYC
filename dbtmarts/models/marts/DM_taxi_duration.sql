select taxi_pk, lpeppickupdatetime, lpepdropoffdatetime, tripduration_hours, tripduration_minutes
from {{ source('BV_source', 'sat_b_taxi') }}