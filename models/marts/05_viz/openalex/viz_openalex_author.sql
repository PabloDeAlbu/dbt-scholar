WITH base AS (
    SELECT 
        *
    FROM {{ref('dim_openalex_author')}}
)

SELECT * FROM base