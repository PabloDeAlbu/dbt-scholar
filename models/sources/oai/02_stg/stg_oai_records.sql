{%- set yaml_metadata -%}
source_model: "ldg_oai_records"
derived_columns:
  source: "!OAI"
  load_datetime: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  record_hk: record_id
  col_hk: col_id
  record_col_hk:
    - record_id
    - col_id
  record_hashdiff:
    is_hashdiff: true
    columns:
      - title
      - date_issued
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
