{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: "map_openalex_work_source"
derived_columns:
  source: "!OPENALEX"
  load_datetime: load_datetime
  {# effective_from: publication_date
  start_date: publication_date
  end_date: to_date('9999-12-31', 'YYYY-MM-DD') #}
hashed_columns:
  work_hk: work_id
  source_hk: source_id
  work_locations_hk:
    - work_id
    - source_id
  work_hashdiff:
    is_hashdiff: true
    columns:
      - source_display_name
      - source_type
      - source_host_organization
      - source_host_organization_name
      - source_is_core
      - source_issn_l
      - landing_page_url
      - license
      - license_id
      - pdf_url
      - version
      - is_accepted
      - is_oa
      - is_published
      - source_is_in_doaj
      - source_is_oa
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
