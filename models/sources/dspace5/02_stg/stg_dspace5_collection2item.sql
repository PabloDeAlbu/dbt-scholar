{%- set yaml_metadata -%}
source_model: "ldg_dspace5_collection2item"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  collection_hk: collection_id
  item_hk: item_id
  collection_item_hk: collection_item_id
  collection2item_hashdiff:
    is_hashdiff: true
    columns:
      - collection_item_id
      - collection_id
      - item_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
