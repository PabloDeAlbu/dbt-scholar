{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_openalex_work"
src_pk: "work_hk"
src_hashdiff:
  source_column: "work_hashdiff"
  alias: "hashdiff"
src_payload:
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
  - apc_list_value
  - apc_list_value_usd
  - apc_paid_currency
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
src_eff: "load_datetime"
src_ldts: "load_datetime"
src_source: "source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
