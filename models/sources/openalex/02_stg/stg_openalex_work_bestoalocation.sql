{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: "base_openalex_work"
derived_columns:
  source: "!OPENALEX"
  load_datetime: load_datetime
  effective_from: publication_date
  start_date: publication_date
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  work_hk: work_id
  work_bestoalocation_hk:
    - work_id
    - best_oa_location_source_id
  work_hashdiff:
    is_hashdiff: true
    columns:
      - best_oa_location_source
      - best_oa_location_source_id
      - best_oa_location_source_display_name
      - best_oa_location_source_type
      - best_oa_location_source_host_organization
      - best_oa_location_source_host_organization_name
      - best_oa_location_landing_page_url
      - best_oa_location_license
      - best_oa_location_license_id
      - best_oa_location_pdf_url
      - best_oa_location_version
      - best_oa_location_source_issn_l
      - best_oa_location_is_accepted
      - best_oa_location_is_oa
      - best_oa_location_is_published
      - best_oa_location_source_is_core
      - best_oa_location_source_is_in_doaj
      - best_oa_location_source_is_indexed_in_scopus
      - best_oa_location_source_is_oa
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
