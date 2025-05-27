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
  primary_location_source_hk: primary_location_source_id
  work_primarylocation_hk:
    - work_id
    - primary_location_source_id
  work_hashdiff:
    is_hashdiff: true
    columns:
      - primary_location_source
      - primary_location_source_id
      - primary_location_source_type
      - primary_location_source_display_name
      - primary_location_source_host_organization
      - primary_location_source_host_organization_name
      - primary_location_landing_page_url
      - primary_location_license
      - primary_location_license_id
      - primary_location_pdf_url
      - primary_location_version
      - primary_location_source_issn_l
      - primary_location_is_accepted
      - primary_location_is_oa
      - primary_location_is_published
      - primary_location_source_is_core
      - primary_location_source_is_in_doaj
      - primary_location_source_is_indexed_in_scopus
      - primary_location_source_is_oa
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
