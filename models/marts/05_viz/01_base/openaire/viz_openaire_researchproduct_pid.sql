WITH base AS (
    SELECT 
        hub_rp.researchproduct_id,
        pid.scheme,
        pid.value
    FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_pid') }} brg USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_pid') }} pid USING (pid_hk)
)

SELECT * FROM base
