{%- set yaml_metadata -%}
source_model: 'raw_tarification'
derived_columns:
  TARIFICATION_KEY: RATECODEID
  RECORD_SOURCE: '!NYC-TAXI'
  LOAD_DATE: DATEADD(DAY,30, LOAD_DATE)
  EFFECTIVE_FROM : LOAD_DATE
hashed_columns:
  TARIFICATIONCODE_PK: RATECODEID
  TAXI_PK: TAXI_ID
  TARIFICATION_TAXI_PK:
    - RATECODEID
    - TAXI_ID
  TARIFICATION_HASHDIFF:
    is_hashdiff: true
    columns:
      - RATECODEID
      - TARIFICATION_DESCRIPTION
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