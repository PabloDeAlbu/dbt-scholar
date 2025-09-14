with base AS (
    SELECT
        hub_rp.researchproduct_id,
        hub_id.original_id,
        hub_rp.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('link_openaire_researchproduct_originalid')}} link_rp USING (researchproduct_hk)
    INNER JOIN {{ref('hub_openaire_originalid')}} hub_id USING (original_hk)
)

SELECT * FROM base
