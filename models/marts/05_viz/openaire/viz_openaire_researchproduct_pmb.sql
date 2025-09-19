{{ config(materialized = 'view') }}

WITH pmb_per_rp AS (
    SELECT 
        hub_rp.researchproduct_id,
        COUNT(*) AS pmb_count
    FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_pid') }} brg USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_pid') }} pid USING (pid_hk)
    WHERE pid.scheme = 'pmb'
    GROUP BY hub_rp.researchproduct_id
),

base AS (
    SELECT 
        rp.researchproduct_id,
        COALESCE(dprp.pmb_count, 0) AS pmb_count,
        CASE
            WHEN COALESCE(dprp.pmb_count, 0) > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_pmb,
        CASE
            WHEN COALESCE(dprp.pmb_count, 0) = 1 THEN TRUE 
            ELSE FALSE 
        END AS has_single_pmb
    FROM {{ ref('hub_openaire_researchproduct') }} rp
    LEFT JOIN pmb_per_rp dprp USING (researchproduct_id)
)

SELECT * FROM base
