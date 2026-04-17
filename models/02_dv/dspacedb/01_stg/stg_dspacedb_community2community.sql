{%- set yaml_metadata -%}
source_model: "ldg_dspacedb_community2community"
derived_columns:
  source: _source_label
  load_datetime: _load_datetime
  effective_from: _load_datetime
  start_date: _load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
  source_label: _source_label
  institution_ror: _institution_ror
hashed_columns:
  parent_comm_hk:
    - _institution_ror
    - _source_label
    - parent_comm_uuid
  child_comm_hk:
    - _institution_ror
    - _source_label
    - child_comm_uuid
  community_community_hk:
    - _institution_ror
    - _source_label
    - parent_comm_uuid
    - child_comm_uuid
  community2community_hashdiff:
    is_hashdiff: true
    columns:
      - _institution_ror
      - _source_label
      - parent_comm_uuid
      - child_comm_uuid
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
