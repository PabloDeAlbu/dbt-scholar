{{ config(materialized = 'table') }}

-- ¿Qué identificadores ORCID fueron registrados por OpenAIRE?
WITH orcid AS (
    SELECT 
        orcid_hk,
        orcid,
        _load_datetime AS load_datetime,
        source
    FROM {{ref('hub_openaire_orcid')}} 
),

-- ¿Qué identificadores ORCID quedan disponibles para relacionarlos con researchproducts?
final AS (
    SELECT
        orcid_hk,
        orcid,
        load_datetime,
        source
    FROM orcid
)

SELECT *
FROM final
