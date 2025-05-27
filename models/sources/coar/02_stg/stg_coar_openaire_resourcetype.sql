{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: 'map_coar_openaire_resourcetype'
derived_columns:
  source: "!SEED"
  load_datetime: load_datetime
hashed_columns:
  coar_hk: coar_uri
  type_hk: type
  link_coar_openaire_hk:
    - coar_uri
    - type
  map_coar_openaire_hashdiff:
    is_hashdiff: false
    columns:
      - label_es
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
