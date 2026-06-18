{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        alternateidentifier_hk,
        researchproduct_alternateidentifier_hk,
        source,
        load_datetime
    FROM {{ref('link_openaire_researchproduct_alternateidentifier')}} 
)

SELECT * FROM base