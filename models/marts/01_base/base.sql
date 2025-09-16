WITH openaire AS (
    SELECT * FROM {{ref('openaire')}}
),

openalex AS (
    SELECT * FROM {{ref('openalex')}}
)

SELECT * FROM openalex
