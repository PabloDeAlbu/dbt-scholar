{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_instances_type_hk,
        researchproduct_hk,
        instancetype_hk
    FROM {{ref('link_openaire_researchproduct_instancetype')}} 
)

SELECT * FROM base
