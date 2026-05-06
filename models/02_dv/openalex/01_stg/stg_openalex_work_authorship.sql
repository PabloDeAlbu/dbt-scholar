{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: 'ldg_openalex_work_authorship'
derived_columns:
  source: "!OPENALEX"
  load_datetime: _load_datetime
hashed_columns:
  work_hk: work_id
  author_hk: author_id
  authorship_hk: authorship_id
  work_authorship_hk:
  - work_id
  - authorship_id
  authorship_hashdiff:
    is_hashdiff: true
    columns:
      - author_display_name
      - author_orcid
      - author_position
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
