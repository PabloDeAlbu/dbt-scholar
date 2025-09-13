{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_openalex_work_locations"
src_pk: "source_hk"
src_hashdiff:
  source_column: "source_hashdiff"
  alias: "hashdiff"
src_payload:
  - source_display_name
  - source_type
  - source_host_organization
  - source_host_organization_name
  - source_is_core
  - source_is_in_doaj
  - source_is_oa
src_eff: "load_datetime"
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
                   source_model=metadata_dict["source_model"])   }}
