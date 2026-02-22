{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: 'ldg_openalex_institution'
derived_columns:
  source: "!OPENALEX"
  load_datetime: dv_load_datetime
hashed_columns:
  institution_hk: institution_id
  ror_hk: ror
  institution_ror_hk:
    - institution_id
    - ror
  institution_hashdiff:
    is_hashdiff: true
    columns:
      - display_name
      - country_code
      - type
      - type_id
      - homepage_url
      - image_url
      - image_thumbnail_url
      - works_count
      - is_super_system
      - works_api_url
      - updated_date
      - created_date
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
