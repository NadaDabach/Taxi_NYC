{%- set source_model = "v_stg_payment" -%}
{%- set src_pk = "PAYMENTTYPE_PK" -%}
{%- set src_hashdiff = {"source_column" : "PAYMENT_HASHDIFF", "alias":"HASHDIFF"} -%}
{%- set src_payload = ["PAYMENT_DESCRIPTION"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}