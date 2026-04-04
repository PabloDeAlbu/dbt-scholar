WITH openaire AS (
    SELECT 
        researchproduct_hk,
        researchproduct_id,
        dim_pid.value as doi
    FROM {{ref('dim_openaire_pid')}} dim_pid
    INNER JOIN {{ref('brg_openaire_researchproduct_pid')}} brg_pid USING (pid_hk)
    INNER JOIN {{ref('dim_openaire_researchproduct')}} USING (researchproduct_hk)
    WHERE dim_pid.scheme = 'doi'
),

openalex AS (
    SELECT 
        work_hk,
        work_id,
        doi 
    FROM {{ref('dim_openalex_work')}}
)

SELECT * 
FROM openaire
FULL OUTER JOIN openalex USING (doi)
