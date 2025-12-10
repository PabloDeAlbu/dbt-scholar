WITH base AS (
    SELECT 
        hub.researchproduct_id,
        orcid,
        CASE WHEN COALESCE(NULLIF(orcid, ''), '-') <> '-' THEN TRUE ELSE FALSE END AS has_orcid,
        publication_date
    FROM {{ ref('hub_openaire_researchproduct') }} hub
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('brg_openaire_researchproduct_orcid') }} brg USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_orcid') }} USING (orcid_hk)
)

SELECT * FROM base
