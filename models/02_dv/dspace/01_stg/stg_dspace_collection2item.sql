{%- set yaml_metadata -%}
source_model: "ldg_dspace_collection2item"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  collection_hk: collection_uuid
  item_hk: item_uuid
  collection_item_hk:
    - collection_uuid
    - item_uuid
  collection2item_hashdiff:
    is_hashdiff: true
    columns:
      - collection_uuid
      - item_uuid
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
