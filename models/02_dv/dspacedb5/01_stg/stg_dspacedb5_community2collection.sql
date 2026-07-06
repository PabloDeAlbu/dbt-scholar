{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_community2collection
derived_columns:
  community_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || community_id::text"
  collection_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || collection_id::text"
  community_collection_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || community_collection_id::text"
  source: source_label
  effective_from: extract_datetime
  start_date: extract_datetime
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  community_hk: community_bk
  collection_hk: collection_bk
  community_collection_hk: community_collection_bk
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
