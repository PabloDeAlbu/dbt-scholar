{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        subject_hk,
        researchproduct_subject_hk
    FROM {{ref('link_openaire_researchproduct_subject')}} 
)

SELECT * FROM base
