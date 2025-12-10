WITH base AS (
    SELECT * FROM {{ref('viz_openaire_researchproduct')}}
)

SELECT * FROM base
