{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "norm_dspace5_item"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: last_modified
  start_date: last_modified
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  item_hk: item_id
  doi_hk: doi
  handle_hk: handle
  type_hk: type
  item_doi_hk:
    - item_id
    - doi
  item_handle_hk:
    - item_id
    - handle
  item_type_hk:
    - item_id
    - type
  item_hashdiff:
    is_hashdiff: true
    columns:
      - item_id
      - title
      - title_lang
      - type
      - doi
      - handle
      - last_modified
      - load_datetime
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
