{%- set yaml_metadata -%}
source_model: 'raw_zones'
derived_columns: 
  ZONES_KEY: OBJECTID
  THE_GEOM: THE_GEOM
  RECORD_SOURCE: '!NYC-TAXI'
  LOAD_DATE: DATEADD(DAY,30, LOAD_DATE)
  EFFECTIVE_FROM : LOAD_DATE
hashed_columns:
  ZONES_PK: OBJECTID
  ZONES_HASHDIFF:
    is_hashdiff: true
    columns:
      - OBJECTID
      - SHAPE_LENG
      - SHAPE_AREA
      - ZONE
      - BOROUGH
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