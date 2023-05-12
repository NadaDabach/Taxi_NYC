{%- set source_model = "v_stg_trip" -%}
{%- set src_pk = "TRIP_PK" -%}
{%- set src_nk = "TRIP_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}