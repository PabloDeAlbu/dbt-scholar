{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: stg_dspace_collection
src_pk: collection_hk
src_hashdiff:
  source_column: collection_hashdiff
  alias: hashdiff
src_payload:
  - collection_uuid
  - submitter
  - template_item_id
  - logo_bitstream_id
  - admin
src_eff: _load_datetime
src_ldts: _load_datetime
src_source: source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
