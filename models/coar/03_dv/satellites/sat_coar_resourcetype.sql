{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_coar_resourcetype"
src_pk: "coar_hk"
src_hashdiff:
  source_column: "resourcetype_hashdiff"
  alias: "hashdiff"
src_payload:
    - label_es
    - label
    - parent_label_1
    - parent_label_2
    - parent_label_3
src_ldts: "load_datetime"
src_source: "source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
