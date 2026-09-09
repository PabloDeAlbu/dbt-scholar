{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_googlescholar_profile"
derived_columns:
  source: "!GOOGLESCHOLAR"
  effective_from: "observed_on::timestamp"
hashed_columns:
  profile_hk: scholar_user_id
  profile_hashdiff:
    is_hashdiff: true
    columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    null_columns=none,
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
