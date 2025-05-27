WITH base AS (
    SELECT 
        *
    FROM {{ref('hub_dspace5_doi')}}
)

SELECT * FROM base