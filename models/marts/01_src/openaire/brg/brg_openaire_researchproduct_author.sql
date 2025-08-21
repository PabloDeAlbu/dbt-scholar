{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        researchproduct_author_hk
    FROM {{ref('link_openaire_researchproduct_author')}} 
)

SELECT * FROM base