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
  doi_hk: doi
  mag_hk: mag
  pmcid_hk: pmcid
  pmid_hk: pmid
  type_hk: type
  work_type_hk:
    - work_id
    - type
  work_ids_hk:
    - work_id
    - doi
    - mag
    - pmcid
    - pmid
  work_hashdiff:
    is_hashdiff: true
    columns:
      - title
      - display_name
      - language
      - type
      - type_crossref
      - fulltext_origin
      - cited_by_api_url
      - has_fulltext
      - is_retracted
      - is_paratext
      - institutions_distinct_count
      - fwci
      - cited_by_count
      - locations_count
      - referenced_works_count
      - countries_distinct_count
      - publication_year
      - publication_date
      - oa_status
      - oa_url
      - any_repository_has_fulltext
      - is_oa
      - apc_list_currency
      - apc_list_provenance
      - apc_list_value
      - apc_list_value_usd
      - apc_paid_currency
      - apc_paid_provenance
      - apc_paid_value
      - apc_paid_value_usd
      - citation_normalized_percentile_is_in_top_10_percent
      - citation_normalized_percentile_is_in_top_1_percent
      - citation_normalized_percentile_value
      - cited_by_percentile_year_max
      - cited_by_percentile_year_min
      - biblio_first_page
      - biblio_issue
      - biblio_last_page
      - biblio_volume
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
