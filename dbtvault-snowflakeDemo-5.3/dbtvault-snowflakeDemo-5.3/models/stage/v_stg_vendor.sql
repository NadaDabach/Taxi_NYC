{%- set yaml_metadata -%}
source_model: 'raw_vendor'
derived_columns:
  VENDOR_KEY: VENDORID
  RECORD_SOURCE: '!NYC-TAXI'
  LOAD_DATE: DATEADD(DAY,30, LOAD_DATE)
  EFFECTIVE_FROM : LOAD_DATE
hashed_columns:
  VENDOR_PK: VENDORID
  TAXI_PK: TAXI_ID
  VENDOR_TAXI_PK:
    - VENDORID
    - TAXI_ID
  VENDOR_HASHDIFF:
    is_hashdiff: true
    columns:
      - VENDORID
      - VENDOR_DESCRIPTION
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