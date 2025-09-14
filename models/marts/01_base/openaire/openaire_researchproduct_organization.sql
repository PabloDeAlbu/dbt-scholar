with base AS (
    SELECT
        hub_rp.researchproduct_id,
        hub_org.organization_id,
        link_rp.researchproduct_hk,
        link_rp.organization_hk,
        link_rp.researchproduct_organization_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('link_openaire_researchproduct_organization')}} link_rp USING (researchproduct_hk)
    INNER JOIN {{ref('hub_openaire_organization')}} hub_org USING (organization_hk)
)

SELECT * FROM base
