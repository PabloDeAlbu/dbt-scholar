{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspace5_collection"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: _load_datetime
  effective_from: to_date('1900-01-01', 'YYYY-MM-DD')
  start_date: to_date('1900-01-01', 'YYYY-MM-DD')
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  collection_hk: collection_id
  submitter_hk: submitter
  collection_submitter_hk:
    - collection_id
    - submitter
  collection_admin_hk:
    - collection_id
    - admin
  collection_hashdiff:
    is_hashdiff: true
    columns:
      - collection_id
      - workflow_step_1
      - workflow_step_2
      - workflow_step_3
      - submitter
      - admin
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
