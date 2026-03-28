{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_dspacedb_item__extract"
src_pk: "item_hk"
src_cdk: "extract_cdk"
src_hashdiff:
  source_column: "item_extract_hashdiff"
  alias: "hashdiff"
src_payload:
  - extract_datetime
  - institution_ror
  - source_label
src_eff: "load_datetime"
src_ldts: "load_datetime"
src_source: "source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(src_pk=metadata_dict["src_pk"],
                      src_cdk=metadata_dict["src_cdk"],
                      src_hashdiff=metadata_dict["src_hashdiff"],
                      src_payload=metadata_dict["src_payload"],
                      src_extra_columns=none,
                      src_eff=metadata_dict["src_eff"],
                      src_ldts=metadata_dict["src_ldts"],
                      src_source=metadata_dict["src_source"],
                      source_model=metadata_dict["source_model"]) }}
