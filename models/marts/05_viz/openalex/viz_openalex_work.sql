WITH base AS (
    SELECT 
        *
    FROM {{ref('fct_openalex_work_publication')}}
)

SELECT * FROM base