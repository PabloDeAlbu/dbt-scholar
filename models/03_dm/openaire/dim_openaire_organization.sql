-- ¿Qué atributos describen el estado vigente de cada organización de OpenAIRE?
WITH organization AS (
    SELECT
        hub_org.organization_id,
        sat_org.acronym,
        sat_org.legalname,
        COALESCE(
            NULLIF(sat_org.legalname, '!UNKNOWN'),
            NULLIF(sat_org.acronym, '!UNKNOWN')
        ) AS organization_name,
        hub_org.organization_hk
    FROM {{ref('hub_openaire_organization')}} hub_org
    INNER JOIN {{ latest_satellite(ref('sat_openaire_organization'), 'organization_hk') }} AS sat_org USING (organization_hk)
),

-- ¿Qué organizaciones quedan disponibles para relacionarlas con researchproducts?
final AS (
    SELECT
        organization_id,
        acronym,
        legalname,
        organization_name,
        organization_hk
    FROM organization
)

SELECT *
FROM final
