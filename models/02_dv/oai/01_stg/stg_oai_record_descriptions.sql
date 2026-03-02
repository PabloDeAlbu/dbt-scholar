{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_oai_record_descriptions"
derived_columns:
  source: "!OAI"
  load_datetime: _load_datetime
  effective_from: _load_datetime
  start_date: _load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  record_hk: record_id
  dc_description_hk: dc_description
  record_description_hk:
    - record_id
    - dc_description
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
