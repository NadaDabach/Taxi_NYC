{%- set source_model = "v_stg_zones" -%}
{%- set src_pk = "ZONES_PK" -%}
{%- set src_hashdiff = {"source_column" : "ZONES_HASHDIFF", "alias":"HASHDIFF"} -%}
{%- set src_payload = ["SHAPE_LENG","THE_GEOM", "SHAPE_AREA", "ZONE", "BOROUGH"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}