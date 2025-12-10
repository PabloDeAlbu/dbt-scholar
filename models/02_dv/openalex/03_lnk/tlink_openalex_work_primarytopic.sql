{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: 'stg_openalex_work_primarytopic'
src_pk: 'work_primarytopic_hk'
src_fk: 
    - 'work_hk'
    - 'topic_hk'
src_payload: 'primarytopic_score'
src_eff: 'updated_date'
src_ldts: 'load_datetime'
src_source: 'source'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(src_pk=metadata_dict["src_pk"],
                      src_fk=metadata_dict["src_fk"],
                      src_payload=metadata_dict["src_payload"],
                      src_eff=metadata_dict["src_eff"],
                      src_ldts=metadata_dict["src_ldts"],
                      src_source=metadata_dict["src_source"],
                      source_model=metadata_dict["source_model"]) }}