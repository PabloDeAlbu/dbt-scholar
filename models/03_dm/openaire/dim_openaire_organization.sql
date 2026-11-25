-- ¿Qué identificador ROR informa OpenAIRE para cada organización?
WITH organization_ror AS (
    SELECT DISTINCT
        organization_hk,
        pid_value AS organization_ror
    FROM {{ ref('stg_openaire_organization_pids') }}
    WHERE LOWER(pid_scheme) = 'ror'
),
-- ¿Qué atributos describen el estado vigente de cada organización de OpenAIRE?
organization AS (
    SELECT
        hub_org.organization_id,
        organization_ror.organization_ror,
        sat_org.acronym,
        sat_org.legalname,
        COALESCE(NULLIF(sat_org.legalname, '!UNKNOWN'), NULLIF(sat_org.acronym, '!UNKNOWN'), hub_org.organization_id) AS organization_name,
        hub_org.organization_hk
    FROM {{ref('hub_openaire_organization')}} hub_org
    INNER JOIN {{ latest_satellite(ref('sat_openaire_organization'), 'organization_hk') }} AS sat_org USING (organization_hk)
    LEFT JOIN organization_ror USING (organization_hk)
),

-- ¿Qué organizaciones quedan disponibles para relacionarlas con researchproducts?
final AS (
    SELECT
        organization_id,
        organization_ror,
        acronym,
        legalname,
        organization_name,
        organization_hk
    FROM organization
)

SELECT *
FROM final
