with base AS (
    SELECT
        hub_org.organization_id,
        sat_org.acronym,
        sat_org.legalname,
        hub_org.organization_hk
    FROM {{ref('hub_openaire_organization')}} hub_org
    INNER JOIN {{ref('sat_openaire_organization')}} sat_org USING (organization_hk)
)

SELECT * FROM base
