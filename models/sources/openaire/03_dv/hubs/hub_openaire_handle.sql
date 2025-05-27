{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: 'stg_openaire_researchproduct_handle'
src_pk: handle_hk
src_nk: handle
src_ldts: load_datetime
src_source: source
{%- endset -%}

WITH base AS (
    {% set metadata_dict = fromyaml(yaml_metadata) %}
    {{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                    src_nk=metadata_dict["src_nk"], 
                    src_ldts=metadata_dict["src_ldts"],
                    src_source=metadata_dict["src_source"],
                    source_model=metadata_dict["source_model"]) }}
),

final AS (
    SELECT *
    FROM base
)

SELECT * FROM final
