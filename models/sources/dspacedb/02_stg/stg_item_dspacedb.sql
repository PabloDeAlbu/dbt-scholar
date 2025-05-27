{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "norm_item_dspacedb"
derived_columns:
  source: "!IR_DSPACEDB"
  load_datetime: load_datetime
  effective_from: last_modified
  start_date: last_modified
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  item_hk: uuid
  doi_hk: doi
  item_doi_hk:
    - uuid
    - doi
  handle_hk: handle
  item_handle_hk:
    - uuid
    - handle
  sal_item_hk:
    - uuid
    - doi
    - handle  
  submitter_hk: submitter_id
  item_submitter_hk:
    - uuid
    - submitter_id
  owningcollection_hk: owning_collection
  item_owningcollection_hk:
    - uuid
    - owning_collection
  type_hk: type
  item_type_hk:
    - uuid
    - type
  item_hashdiff:
    is_hashdiff: true
    columns:
      - in_archive
      - withdrawn
      - last_modified
      - discoverable
      - uuid
      - submitter_id
      - owning_collection
      - load_datetime
      - title
      - title_lang
      - type
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
