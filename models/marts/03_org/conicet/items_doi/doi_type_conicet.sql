WITH base as (
    SELECT 
        doi,
        type 
    FROM {{ref('fct_openalex_work')}} 
),

coar AS (
    SELECT 
        type, 
        label, 
        coar_uri, 
        label_es
    FROM {{ref('seed_coar_openalex')}}
)

SELECT 
    base.doi, 
    coar.label_es
FROM base
INNER JOIN coar ON
    base.type = coar.type
ORDER BY RANDOM()
LIMIT 3000

