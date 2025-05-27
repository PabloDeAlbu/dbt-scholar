{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "base_openaire_researchproduct_datasource"
derived_columns:
  source: "!OPENAIRE"
  load_datetime: load_datetime
hashed_columns:
  researchproduct_hk: researchproduct_id
  datasource_hk: datasource_id
  researchproduct_datasource_hk:
    - researchproduct_id
    - datasource_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
