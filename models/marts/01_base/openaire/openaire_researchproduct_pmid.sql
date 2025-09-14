with base AS (
    SELECT
        hub_rp.researchproduct_id,
        hub_pid.value,
        hub_rp.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('link_openaire_researchproduct_pid')}} link_rp USING (researchproduct_hk)
    INNER JOIN {{ref('hub_openaire_pid')}} hub_pid USING (pid_hk)
    WHERE scheme = 'pmid'
)

SELECT * FROM base
