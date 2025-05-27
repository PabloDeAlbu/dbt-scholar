{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: 'map_openalex_author_institution'
derived_columns:
  source: "!OPENALEX"
  load_datetime: load_datetime
hashed_columns:
  author_hk: author_id
  institution_hk: institution_id
  ror_hk: ror
  type_hk: type
  institution_ror_hk:
    - institution_id
    - ror
  author_institution_hk:
    - author_id
    - institution_id
  institution_type_hk:
    - institution_id
    - type
  institution_hashdiff:
    is_hashdiff: true
    columns:
      - country_code
      - display_name
      - ror
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
