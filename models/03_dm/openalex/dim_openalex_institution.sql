{{ config(materialized = 'table') }}

-- ¿Qué atributos describen el estado vigente de cada institución de OpenAlex?
WITH institution AS (
    SELECT
        hub_institution.institution_id,
        hub_institution.institution_hk,
        COALESCE(
            NULLIF(sat_institution.display_name, '!UNKNOWN'),
            NULLIF(sat_institution_dehydrated.display_name, '!UNKNOWN')
        ) AS institution_display_name,
        COALESCE(NULLIF(sat_institution.country_code, '!UNKNOWN'), '-') AS country_code
    FROM {{ref('hub_openalex_institution')}} hub_institution
    LEFT JOIN {{ latest_satellite(ref('sat_openalex_institution'), 'institution_hk') }} AS sat_institution USING (institution_hk)
    LEFT JOIN {{ latest_satellite(ref('sat_openalex_institution_dehydrated'), 'institution_hk') }} AS sat_institution_dehydrated USING (institution_hk)
),

-- ¿Qué relaciones entre instituciones y ROR conserva OpenAlex?
institution_ror_relationship AS (
    SELECT DISTINCT
        institution_hk,
        ror
    FROM {{ ref('brg_openalex_institution_ror') }}
),

-- ¿Cuántos ROR se relacionan con cada institución de OpenAlex?
institution_ror_count AS (
    SELECT
        institution_hk,
        COUNT(DISTINCT ror) AS ror_count
    FROM institution_ror_relationship
    GROUP BY institution_hk
),

-- ¿Qué ROR pueden atribuirse sin ambigüedad a una institución de OpenAlex?
institution_ror_unambiguous AS (
    SELECT
        relationship.institution_hk,
        relationship.ror
    FROM institution_ror_relationship relationship
    INNER JOIN institution_ror_count
        USING (institution_hk)
    WHERE institution_ror_count.ror_count = 1
),

-- ¿Qué atributos expone la dimensión de instituciones de OpenAlex?
final AS (
    SELECT
        institution.institution_id,
        institution.institution_hk,
        institution.institution_display_name,
        institution.country_code,
        COALESCE(institution_ror_unambiguous.ror, '-') AS ror,
        COALESCE(institution_ror_count.ror_count, 0) AS ror_count,
        COALESCE(institution_ror_count.ror_count, 0) > 0 AS has_ror,
        COALESCE(institution_ror_count.ror_count, 0) = 1 AS has_single_ror
    FROM institution
    LEFT JOIN institution_ror_count
        USING (institution_hk)
    LEFT JOIN institution_ror_unambiguous
        USING (institution_hk)
)

SELECT *
FROM final
