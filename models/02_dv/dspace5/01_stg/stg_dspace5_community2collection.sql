{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspace5_community2collection"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: to_date('1900-01-01', 'YYYY-MM-DD')
  start_date: to_date('1900-01-01', 'YYYY-MM-DD')
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  community_hk: community_id
  collection_hk: collection_id
  community_collection_hk: community_collection_id
  community2collection_hashdiff:
    is_hashdiff: true
    columns:
      - community_collection_id
      - community_id
      - collection_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
