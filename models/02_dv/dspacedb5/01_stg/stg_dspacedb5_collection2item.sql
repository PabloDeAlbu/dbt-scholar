{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_collection2item
derived_columns:
  collection_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || collection_id::text"
  item_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || item_id::text"
  collection_item_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || collection_item_id::text"
  source: source_label
  effective_from: extract_datetime
  start_date: extract_datetime
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  collection_hk: collection_bk
  item_hk: item_bk
  collection_item_hk: collection_item_bk
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
