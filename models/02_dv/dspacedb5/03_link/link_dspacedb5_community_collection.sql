{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: stg_dspacedb5_community2collection
src_pk: community_collection_hk
src_fk:
  - community_hk
  - collection_hk
src_ldts: load_datetime
src_source: source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(src_pk=metadata_dict["src_pk"],
                    src_fk=metadata_dict["src_fk"],
                    src_ldts=metadata_dict["src_ldts"],
                    src_source=metadata_dict["src_source"],
                    source_model=metadata_dict["source_model"]) }}
