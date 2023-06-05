{%- set source_model = "v_stg_tarification" -%}
{%- set src_pk = "TARIFICATIONCODE_PK" -%}
{%- set src_nk = "TARIFICATION_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}