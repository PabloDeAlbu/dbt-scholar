with base AS (
    SELECT
        hub_rp.researchproduct_id,
        hub_orcid.orcid,
        link_rp.researchproduct_hk,
        link_rp.orcid_hk,
        link_rp.researchproduct_orcid_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('link_openaire_researchproduct_orcid')}} link_rp USING (researchproduct_hk)
    INNER JOIN {{ref('hub_openaire_orcid')}} hub_orcid USING (orcid_hk)
)

SELECT * FROM base
