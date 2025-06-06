{%- set yaml_metadata -%}
source_model: "ldg_dspace5_community"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  community_hk: community_id
  admin_hk: admin
  community_admin_hk: 
    - community_id
    - admin
  community_hashdiff:
    is_hashdiff: true
    columns:
      - community_id
      - admin
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
