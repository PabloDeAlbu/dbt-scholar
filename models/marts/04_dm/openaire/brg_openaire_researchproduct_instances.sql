{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_collectedfrom_hk,
        researchproduct_hk,
        collectedfrom_hk
    FROM {{ref('link_openaire_researchproduct_instances_collectedfrom')}} 
)

SELECT * FROM base
