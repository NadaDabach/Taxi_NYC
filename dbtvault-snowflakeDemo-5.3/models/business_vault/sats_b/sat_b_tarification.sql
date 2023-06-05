with t1 as (
    select tarificationcode_pk, count(*) as total_taxi
    from {{ ref('link_tarification_taxi') }}
    group by tarificationcode_pk
)
select t1.tarificationcode_pk, s.hashdiff, s.tarification_description, t1.total_taxi, s.effective_from, s.load_date, s.record_source
from t1 join {{ ref('sat_tarification') }} as s on t1.tarificationcode_pk = s.tarificationcode_pk