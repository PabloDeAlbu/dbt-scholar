{{ config(materialized='table') }}

-- ¿Qué relaciones entre instituciones y ROR conserva OpenAlex?
WITH relationship AS (
    SELECT
        institution_ror_hk,
        institution_hk,
        ror_hk
    FROM {{ ref('link_openalex_institution_ror') }}
),

-- ¿Qué valor corresponde a cada ROR relacionado?
ror AS (
    SELECT
        ror_hk,
        ror
    FROM {{ ref('hub_openalex_ror') }}
    WHERE ror NOT IN ('!UNKNOWN', '-')
      AND ror != ''
),

-- ¿Qué ROR están asociados con cada institución de OpenAlex?
final AS (
    SELECT
        relationship.institution_ror_hk,
        relationship.institution_hk,
        relationship.ror_hk,
        ror.ror
    FROM relationship
    INNER JOIN ror
        USING (ror_hk)
)

SELECT *
FROM final
