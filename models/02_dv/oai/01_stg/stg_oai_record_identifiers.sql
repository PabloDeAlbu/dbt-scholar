{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_oai_record_identifiers"
derived_columns:
  source: "!OAI"
  load_datetime: dv_load_datetime
  effective_from: dv_load_datetime
  start_date: dv_load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  record_hk: record_id
  dc_identifier_hk: dc_identifier
  record_identifier_hk:
    - record_id
    - dc_identifier
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
