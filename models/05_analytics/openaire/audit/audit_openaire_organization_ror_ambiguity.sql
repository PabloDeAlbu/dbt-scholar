{{ config(materialized='view') }}

-- ¿Qué relaciones entre organizaciones y ROR conserva el bridge de PIDs?
WITH organization_ror AS (
    SELECT
        organization_hk,
        value AS organization_ror
    FROM {{ ref('brg_openaire_organization_pid') }}
    WHERE LOWER(scheme) = 'ror'
      AND value != '!UNKNOWN'
),

-- ¿Qué organizaciones están relacionadas con más de un ROR distinto?
ambiguous_organization AS (
    SELECT
        organization_hk,
        COUNT(DISTINCT organization_ror) AS organization_ror_count
    FROM organization_ror
    GROUP BY organization_hk
    HAVING COUNT(DISTINCT organization_ror) > 1
),

-- ¿Qué atributos vigentes permiten interpretar cada organización ambigua?
organization AS (
    SELECT
        hub.organization_hk,
        hub.organization_id,
        sat.acronym,
        sat.legalname,
        COALESCE(
            NULLIF(sat.legalname, '!UNKNOWN'),
            NULLIF(sat.acronym, '!UNKNOWN'),
            hub.organization_id
        ) AS organization_name
    FROM {{ ref('hub_openaire_organization') }} AS hub
    INNER JOIN {{ latest_satellite(ref('sat_openaire_organization'), 'organization_hk') }} AS sat
        USING (organization_hk)
),

-- ¿Qué ROR compiten por representar cada organización ambigua de OpenAIRE?
final AS (
    SELECT
        organization.organization_id,
        organization.organization_name,
        organization.acronym,
        organization.legalname,
        organization_ror.organization_ror,
        ambiguous_organization.organization_ror_count,
        organization.organization_hk
    FROM ambiguous_organization
    INNER JOIN organization_ror
        USING (organization_hk)
    INNER JOIN organization
        USING (organization_hk)
)

SELECT *
FROM final
