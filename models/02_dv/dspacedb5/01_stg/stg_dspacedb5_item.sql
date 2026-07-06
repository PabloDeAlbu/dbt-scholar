{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_item
derived_columns:
  scope_bk: "institution_ror || '||' || source_label || '||' || base_url"
  item_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || item_id::text"
  owningcollection_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || owning_collection::text"
  source: source_label
  effective_from: "COALESCE(last_modified, extract_datetime, load_datetime)"
  start_date: "COALESCE(last_modified, extract_datetime, load_datetime)"
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  scope_hk: scope_bk
  item_hk: item_bk
  submitter_hk: submitter_id
  owningcollection_hk: owningcollection_bk
  item_owningcollection_hk:
    - item_bk
    - owningcollection_bk
  item_hashdiff:
    is_hashdiff: true
    columns:
      - item_bk
      - submitter_id
      - in_archive
      - withdrawn
      - last_modified
      - owning_collection
      - discoverable
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
