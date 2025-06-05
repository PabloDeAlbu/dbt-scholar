{%- set yaml_metadata -%}
source_model: "ldg_oai_item_by_set"
derived_columns:
  source: "!OAI"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  item_hk: item_id
  handle_hk: handle
  col_hk: col_id
  item_handle_hk:
    - item_id
    - handle
  item_col_hk:
    - item_id
    - col_id
  item_hashdiff:
    is_hashdiff: true
    columns:
      - item_id
      - handle
      - col_id
      - title
      - date_issued
      - type_openaire
      - type_snrd
      - version
      - access_right
      - license_condition
      - load_datetime
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
