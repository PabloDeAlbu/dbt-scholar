{{ config(materialized = 'view') }}

WITH handle_per_rp AS (
    SELECT 
        hub_rp.researchproduct_id,
        COUNT(*) AS handle_count
    FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_pid') }} brg USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_pid') }} pid USING (pid_hk)
    WHERE pid.scheme = 'handle'
    GROUP BY hub_rp.researchproduct_id
),

base AS (
    SELECT 
        rp.researchproduct_id,
        COALESCE(dprp.handle_count, 0) AS handle_count,
        CASE
            WHEN COALESCE(dprp.handle_count, 0) > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_handle,
        CASE
            WHEN COALESCE(dprp.handle_count, 0) = 1 THEN TRUE 
            ELSE FALSE 
        END AS has_single_handle
    FROM {{ ref('hub_openaire_researchproduct') }} rp
    LEFT JOIN handle_per_rp dprp USING (researchproduct_id)
)

SELECT * FROM base
