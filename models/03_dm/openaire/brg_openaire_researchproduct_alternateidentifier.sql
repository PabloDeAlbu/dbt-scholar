{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts e identificadores alternativos conserva OpenAIRE?
WITH relationship AS (
    SELECT
        researchproduct_hk,
        alternateidentifier_hk,
        researchproduct_alternateidentifier_hk,
        source,
        load_datetime
    FROM {{ ref('link_openaire_researchproduct_alternateidentifier') }}
),

-- ¿Qué esquema y valor corresponden a cada identificador alternativo relacionado?
alternate_identifier AS (
    SELECT
        alternateidentifier_hk,
        scheme,
        value
    FROM {{ ref('hub_openaire_alternateidentifier') }}
),

-- ¿Qué identificadores alternativos están asociados con cada researchproduct?
final AS (
    SELECT
        relationship.researchproduct_alternateidentifier_hk,
        relationship.researchproduct_hk,
        relationship.alternateidentifier_hk,
        alternate_identifier.scheme,
        alternate_identifier.value,
        relationship.source,
        relationship.load_datetime
    FROM relationship
    INNER JOIN alternate_identifier
        USING (alternateidentifier_hk)
)

SELECT *
FROM final
