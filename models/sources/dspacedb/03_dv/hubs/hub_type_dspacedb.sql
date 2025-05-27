{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: 'stg_item_dspacedb'
src_pk: type_hk
src_nk: type
src_ldts: load_datetime
src_source: source
{%- endset -%}

with base as (
    {% set metadata_dict = fromyaml(yaml_metadata) %}
    {{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                    src_nk=metadata_dict["src_nk"], 
                    src_ldts=metadata_dict["src_ldts"],
                    src_source=metadata_dict["src_source"],
                    source_model=metadata_dict["source_model"]) }}
),

final as (
    select * 
    from base
)

select * from final
