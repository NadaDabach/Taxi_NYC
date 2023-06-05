{%- set yaml_metadata -%}
source_model: 'raw_taxi_NYC'
derived_columns:
  TAXI_KEY: ID
  RECORD_SOURCE: '!NYC-TAXI'
  LOAD_DATE: DATEADD(DAY,30, LOAD_DATE)
  EFFECTIVE_FROM : LOAD_DATE
hashed_columns:
  TAXI_PK: ID
  PULOCATION_PK: PULOCATIONID
  DOLOCATION_PK: DOLOCATIONID
  TAXI_ZONES_PU_PK:
    - PULOCATIONID
    - ID
  TAXI_ZONES_DO_PK:
    - DOLOCATIONID
    - ID
  TAXI_HASHDIFF:
    is_hashdiff: true
    columns:
      - ID
      - LPEPPICKUPDATETIME
      - LPEPDROPOFFDATETIME
      - PASSENGERCOUNT
      - TRIPDISTANCE
      - STOREANDFWDFLAG
      - FAREAMOUNT
      - EXTRA
      - MTATAX
      - IMPROVEMENTSURCHARGE
      - TIPAMOUNT
      - TOLLSAMOUNT
      - TOTALAMOUNT
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