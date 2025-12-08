WITH base AS (
    SELECT * FROM {{ref('viz_openalex_work')}}
)

SELECT * FROM base
