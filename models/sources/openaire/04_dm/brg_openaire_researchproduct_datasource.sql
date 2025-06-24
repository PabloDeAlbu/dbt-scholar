{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        researchproduct_datasource_hk
    FROM {{ref('link_openaire_researchproduct_datasource')}} 
)

SELECT * FROM base