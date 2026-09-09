{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_googlescholar_profile_publication"
derived_columns:
  source: "!GOOGLESCHOLAR"
  effective_from: "observed_on::timestamp"
hashed_columns:
  profile_hk: scholar_user_id
  profile_publication_hk:
    - scholar_user_id
    - scholar_publication_id
  profile_publication_hashdiff:
    is_hashdiff: true
    columns:
      - position
      - title
      - authors
      - publication_details
      - publication_year
      - cited_by_count
      - googlescholar_cluster_id
      - publication_url
      - cluster_url
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
