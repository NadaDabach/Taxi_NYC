with t1 as (
    select s_pk, count(*) as total_taxi
    from {{ ref('link_stockandtransmission_taxi') }}
    group by s_pk
)
select t1.s_pk, s.hashdiff, s.s_description, t1.total_taxi, s.effective_from, s.load_date, s.record_source
from t1 join {{ ref('sat_stockandtransmission') }} as s on t1.s_pk = s.s_pk