{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_openaire_researchproduct_alternateidentifiers"
derived_columns:
  source: "!OPENAIRE"
  load_datetime: _load_datetime
hashed_columns:
  researchproduct_hk: researchproduct_id
  alternateidentifier_hk:    
    - scheme
    - value
  researchproduct_alternateidentifier_hk:
    - researchproduct_id
    - scheme
    - value
  researchproduct_alternateidentifier_hk:
    is_hashdiff: true
    columns:
      - scheme
      - value 
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
