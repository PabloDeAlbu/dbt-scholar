{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspace5_community2community"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: to_date('1900-01-01', 'YYYY-MM-DD')
  start_date: to_date('1900-01-01', 'YYYY-MM-DD')
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  parent_comm_hk: parent_comm_id
  child_comm_hk: child_comm_id
  community_community_hk: community_community_id
  community2community_hashdiff:
    is_hashdiff: true
    columns:
      - community_community_id
      - parent_comm_id
      - child_comm_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
