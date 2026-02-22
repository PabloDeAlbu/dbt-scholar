{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_openaire_researchproduct_instances"
derived_columns:
  source: "!OPENAIRE"
  load_datetime: dv_load_datetime
hashed_columns:
  researchproduct_hk: researchproduct_id
  collectedfrom_hk: collectedfrom_key
  hostedby_hk: hostedby_key
  instancetype_hk: type
  url_hk: url
  researchproduct_collectedfrom_hk: 
    - researchproduct_id
    - collectedfrom_key
  researchproduct_hostedby_hk: 
    - researchproduct_id
    - hostedby_key
  researchproduct_instances_type_hk:
    - researchproduct_id
    - type
  researchproduct_url_hk:
    - researchproduct_id
    - url
  researchproduct_collectedfrom_hashdiff:
    is_hashdiff: true
    columns:
      - license
      - publication_date
      - refereed
      - type
      - url
      - accessright_code
      - accessright_label
      - accessright_openaccessroute
      - accessright_scheme
      - apc_amount
      - apc_currency
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
