{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        organization_hk,
        researchproduct_organization_hk
    FROM {{ref('link_openaire_researchproduct_organization')}} 
)

SELECT * FROM base