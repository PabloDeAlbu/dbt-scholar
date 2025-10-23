{# TODO #}
{{ config(materialized='table') }}

WITH final as (
    SELECT 
        fct.date_accessioned, 
        COUNT(*) cantidad
    FROM {{ref('fct_sedici_item_accessioned')}} fct
    GROUP BY 1
    ORDER BY 1
)

SELECT * FROM final
