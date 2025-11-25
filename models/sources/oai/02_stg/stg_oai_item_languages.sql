{%- set yaml_metadata -%}
source_model: "ldg_oai_item_languages"
derived_columns:
  source: "!OAI"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  item_hk: item_id
  language_hk: languages
  item_language_hk: 
    - item_id
    - languages

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
