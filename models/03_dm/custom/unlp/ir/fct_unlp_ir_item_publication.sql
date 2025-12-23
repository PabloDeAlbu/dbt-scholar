{{ config(materialized = 'table') }}

WITH 
base as (
    SELECT DISTINCT
        *
    FROM {{ref('er_dspace5_item')}} fct
),
final AS (
    SELECT 
        base.*
    FROM base
)

SELECT * FROM final