with t1 as (
    select vendor_pk, count(*) as total_taxi
    from {{ ref('link_vendor_taxi') }}
    group by vendor_pk
)
select t1.vendor_pk, s.hashdiff, s.vendor_description, t1.total_taxi, s.effective_from, s.load_date, s.record_source
from t1 join {{ ref('sat_vendor') }} as s on t1.vendor_pk = s.vendor_pk