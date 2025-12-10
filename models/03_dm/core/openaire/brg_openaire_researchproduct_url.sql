{{ config(materialized = 'table') }}

WITH base as (
    SELECT
        researchproduct_hk,
        url_hk,
        researchproduct_url_hk
    FROM {{ref('link_openaire_researchproduct_url')}} 
)

SELECT * FROM base