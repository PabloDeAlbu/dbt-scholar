{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        value,
        scheme,
        researchproduct_hk,
        pid_hk,
        researchproduct_pid_hk
    FROM {{ref('link_openaire_researchproduct_pid')}} 
    JOIN {{ref('hub_openaire_pid')}} USING (pid_hk)
)

SELECT * FROM base