{%- set source_model = ["v_stg_poi"] -%}
{%- set src_pk = "POI_ZONES_PK" -%}
{%- set src_fk = ["POI_PK", "ZONE_PK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}