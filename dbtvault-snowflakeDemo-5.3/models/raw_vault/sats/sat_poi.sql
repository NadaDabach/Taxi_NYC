{%- set source_model = "v_stg_poi" -%}
{%- set src_pk = "POI_PK" -%}
{%- set src_hashdiff = {"source_column" : "POI_HASHDIFF", "alias":"HASHDIFF"} -%}
{%- set src_payload = ["SEGMENTID","COMPLEXID", "SAFTYPE", "SOS", "FACI_DOM","BIN","BOROUGH","CREATED","MODIFIED","FACILITY_T","SOURCE","B7SC","PRI_ADD","NAME"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}