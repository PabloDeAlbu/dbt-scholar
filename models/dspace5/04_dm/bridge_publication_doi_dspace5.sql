WITH base AS (
    SELECT 
        *
    FROM {{ref('link_dspace5_item_doi')}}
)

SELECT * FROM base