with t1 as (
    select paymenttype_pk, count(*) as total_taxi
    from {{ ref('link_payment_taxi') }}
    group by paymenttype_pk
)
select t1.paymenttype_pk, s.hashdiff, s.payment_description, t1.total_taxi, s.effective_from, s.load_date, s.record_source
from t1 join {{ ref('sat_payment') }} as s on t1.paymenttype_pk = s.paymenttype_pk