WITH base AS (
    SELECT 
        hub_identifier.identifiers,
        hub_identifier.identifier_hk
    FROM {{ ref('hub_oai_identifier') }} hub_identifier
)

SELECT * FROM base
