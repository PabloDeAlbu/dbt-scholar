{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_dspacedb_metadatavalue"
src_pk: "metadatavalue_hk"
src_hashdiff:
  source_column: "metadatavalue_hashdiff"
  alias: "hashdiff"
src_payload:
  - metadatafield_hk
  - dspaceobject_hk
  - metadatavalue_bk
  - metadatafield_bk
  - dspaceobject_bk
  - metadata_value_id
  - metadata_field_id
  - text_value
  - text_lang
  - place
  - authority
  - confidence
  - dspace_object_id
  - base_url
  - source_label
  - institution_ror
  - extract_datetime
src_eff: "effective_from"
src_ldts: "load_datetime"
src_source: "source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
