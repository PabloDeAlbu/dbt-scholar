{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: "ldg_openalex_work"
derived_columns:
  source: "!OPENALEX"
  load_datetime: load_datetime
  effective_from: publication_date
  start_date: publication_date
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  work_hk: work_id
  location_source_hk: location_source_id
  work_primarylocation_hk:
    - work_id
    - location_source_id
  location_hashdiff:
    is_hashdiff: true
    columns:
      - location_source
      - location_source_id
      - location_source_type
      - location_source_display_name
      - location_source_host_organization
      - location_source_host_organization_name
      - location_landing_page_url
      - location_license
      - location_license_id
      - location_pdf_url
      - location_version
      - location_source_issn_l
      - location_is_accepted
      - location_is_oa
      - location_is_published
      - location_source_is_core
      - location_source_is_in_doaj
      - location_source_is_indexed_in_scopus
      - location_source_is_oa
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
