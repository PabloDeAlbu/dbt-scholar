WITH base AS (
    SELECT 
        *
    FROM {{ref('link_openalex_work_bestoalocation_source')}} link_work_bestoalocation_source
)

SELECT * FROM base