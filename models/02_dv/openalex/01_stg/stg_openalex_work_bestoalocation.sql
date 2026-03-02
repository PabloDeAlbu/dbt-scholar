{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: "ldg_openalex_work"
derived_columns:
  source: "!OPENALEX"
  load_datetime: _load_datetime
  effective_from: publication_date
  start_date: publication_date
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  work_hk: work_id
  source_hk: best_oa_location_source_id
  work_bestoalocation_hk:
    - work_id
    - best_oa_location_source_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
