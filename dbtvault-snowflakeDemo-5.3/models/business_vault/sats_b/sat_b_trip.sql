with t1 as (
    select trip_pk, count(*) as total_taxi
    from {{ ref('link_trip_taxi') }}
    group by trip_pk
)
select t1.trip_pk, s.hashdiff, s.trip_description, t1.total_taxi, s.effective_from, s.load_date, s.record_source
from t1 join {{ ref('sat_trip') }} as s on t1.trip_pk = s.trip_pk