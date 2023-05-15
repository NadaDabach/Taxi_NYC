{%- set yaml_metadata -%}
source_model: 'raw_pointOfInterest'
derived_columns:
  POI_KEY: PLACEID
  POI_GEOM: POI_GEOM
  RECORD_SOURCE: '!NYC-TAXI'
  LOAD_DATE: DATEADD(DAY,30, LOAD_DATE)
  EFFECTIVE_FROM : LOAD_DATE
hashed_columns:
  POI_PK: PLACEID
  ZONE_PK: OBJECTID
  POI_ZONES_PK:
    - PLACEID
    - OBJECTID
  POI_HASHDIFF:
    is_hashdiff: true
    columns:
      - PLACEID
      - SEGMENTID
      - COMPLEXID
      - SAFTYPE
      - SOS
      - FACI_DOM
      - BIN
      - BOROUGH
      - CREATED
      - MODIFIED
      - FACILITY_T
      - SOURCE
      - B7SC
      - PRI_ADD
      - NAME
      - EFFECTIVE_FROM
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}