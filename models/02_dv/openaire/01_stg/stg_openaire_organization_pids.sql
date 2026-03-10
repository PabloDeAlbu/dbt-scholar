{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_openaire_map_organization_pid"
derived_columns:
  source: "!OPENAIRE"
  load_datetime: _load_datetime
hashed_columns:
  organization_hk: organization_id
  pid_hk:
    - pid_scheme
    - pid_value
  organization_pid_hk:
    - organization_id
    - pid_scheme
    - pid_value  
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
