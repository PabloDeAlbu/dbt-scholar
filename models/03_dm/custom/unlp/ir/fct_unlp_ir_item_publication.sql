{{ config(materialized = 'table') }}

WITH 
base as (
    SELECT DISTINCT
        *
    FROM {{ref('fct_dspace5_item_publication')}} fct
),
final AS (
    SELECT 
        base.*
    FROM base
)

SELECT * FROM final