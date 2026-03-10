
    
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_openaire_map_researchproduct_subject"
derived_columns:
  source: "!OPENAIRE"
  load_datetime: _load_datetime
hashed_columns:
  researchproduct_hk: researchproduct_id
  subject_hk:
    - subject_scheme
    - subject_value
  researchproduct_subject_hk:
    - researchproduct_id
    - subject_scheme
    - subject_value
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
