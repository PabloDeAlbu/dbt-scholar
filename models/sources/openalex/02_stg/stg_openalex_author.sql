{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: "base_openalex_author"
derived_columns:
  source: "!OPENALEX"
  load_datetime: load_datetime
hashed_columns:
  author_hk: author_id
  orcid_hk: orcid
  author_orcid_hk:
  - author_id
  - orcid
  author_hashdiff:
    is_hashdiff: true
    columns:
      - display_name
      - works_count
      - cited_by_count

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
