{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        original_hk,
        researchproduct_originalid_hk
    FROM {{ref('link_openaire_researchproduct_originalid')}} 
)

SELECT * FROM base