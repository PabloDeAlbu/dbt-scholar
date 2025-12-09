WITH base AS (
    SELECT 
        hub_language.languages,
        hub_language.language_hk
    FROM {{ ref('hub_oai_language') }} hub_language
)

SELECT * FROM base
