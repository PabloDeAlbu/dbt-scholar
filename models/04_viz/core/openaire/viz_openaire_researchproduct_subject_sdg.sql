WITH base AS (
    SELECT
        hub_rp.researchproduct_id,
        brg_sdg.subject_value,
        CASE
            WHEN brg_sdg.subject_hk IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_sdg,
        hub_rp.researchproduct_hk
    FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_subject_sdg') }} brg_sdg USING (researchproduct_hk)
)

SELECT * FROM base
