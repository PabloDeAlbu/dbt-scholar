{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts y URLs conserva el Data Vault de OpenAIRE?
WITH relationship AS (
    SELECT
        researchproduct_hk,
        url_hk,
        researchproduct_url_hk
    FROM {{ ref('link_openaire_researchproduct_url') }}
),

-- ¿Qué valor de URL corresponde a cada clave relacionada?
url AS (
    SELECT
        url_hk,
        url
    FROM {{ ref('hub_openaire_url') }}
),

-- ¿Qué URLs quedan disponibles para cada researchproduct de OpenAIRE?
final AS (
    SELECT
        relationship.researchproduct_url_hk,
        relationship.researchproduct_hk,
        relationship.url_hk,
        url.url
    FROM relationship
    INNER JOIN url
        USING (url_hk)
)

SELECT *
FROM final
