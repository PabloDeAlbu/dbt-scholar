{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        researchproduct_hk,
        pid_hk,
        researchproduct_pid_hk
    FROM {{ref('link_openaire_researchproduct_pid')}} 
)

SELECT * FROM base