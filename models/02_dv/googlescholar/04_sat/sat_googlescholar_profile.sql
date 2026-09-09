{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_googlescholar_profile"
src_pk: profile_hk
src_hashdiff:
  source_column: profile_hashdiff
  alias: hashdiff
src_payload:
  - profile_name
  - profile_url
  - orcid
  - affiliation
  - organization_name
  - organization_url
  - verified_email
  - interests
  - metrics_since_year
  - cited_by_count_total
  - cited_by_count_since
  - h_index_total
  - h_index_since
  - i10_index_total
  - i10_index_since
  - observed_on
  - source_file
src_eff: effective_from
src_ldts: load_datetime
src_source: source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(
    src_pk=metadata_dict['src_pk'],
    src_hashdiff=metadata_dict['src_hashdiff'],
    src_payload=metadata_dict['src_payload'],
    src_eff=metadata_dict['src_eff'],
    src_ldts=metadata_dict['src_ldts'],
    src_source=metadata_dict['src_source'],
    source_model=metadata_dict['source_model']
) }}
