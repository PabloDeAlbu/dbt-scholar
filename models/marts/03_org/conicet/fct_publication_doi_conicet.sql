WITH base AS (
    SELECT * FROM {{ ref('ldg_openalex_work') }}
)

SELECT * FROM base