
WITH pmc_per_rp AS (
    SELECT 
        hub_rp.researchproduct_id,
        hub_rp.researchproduct_hk,
        COUNT(*) AS pmc_count
    FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_pid') }} brg USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_pid') }} pid USING (pid_hk)
    WHERE pid.scheme = 'pmc'
    GROUP BY 1, 2
),

base AS (
    SELECT 
        rp.researchproduct_id,
        rp.researchproduct_hk,
        COALESCE(dprp.pmc_count, 0) AS pmc_count,
        CASE
            WHEN COALESCE(dprp.pmc_count, 0) > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_pmc,
        CASE
            WHEN COALESCE(dprp.pmc_count, 0) = 1 THEN TRUE 
            ELSE FALSE 
        END AS has_single_pmc
    FROM {{ ref('hub_openaire_researchproduct') }} rp
    LEFT JOIN pmc_per_rp dprp USING (researchproduct_id)
)

SELECT * FROM base
