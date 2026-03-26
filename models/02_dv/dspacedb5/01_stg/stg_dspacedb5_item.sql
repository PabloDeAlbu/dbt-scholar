{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspacedb5_item"
derived_columns:
  source: _source_label
  load_datetime: _load_datetime
  extract_datetime: _extract_datetime
  effective_from: COALESCE(last_modified, _extract_datetime, _load_datetime)
  start_date: COALESCE(last_modified, _extract_datetime, _load_datetime)
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
  item_bk: "COALESCE(_institution_ror::text, '') || '||' || COALESCE(_source_label::text, '') || '||' || COALESCE(item_id::text, '')"
  owningcollection_bk: "COALESCE(_institution_ror::text, '') || '||' || COALESCE(_source_label::text, '') || '||' || COALESCE(owning_collection::text, '')"
hashed_columns:
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

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
