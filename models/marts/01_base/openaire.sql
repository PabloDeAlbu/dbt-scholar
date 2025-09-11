WITH base AS (
    SELECT 
        hub_rp.researchproduct_id,
        sat_rp.main_title,
        {# as subtitle, #}
        sat_rp.type,
        sat_rp.publication_date as date,
        hub_pid.value as doi,
        STRING_AGG(hub_author.full_name, '|' ORDER BY hub_author.rank) as author
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp 
    INNER JOIN {{ref('link_openaire_researchproduct_pid')}} link_rp_pid USING (researchproduct_hk)
    LEFT JOIN {{ref('hub_openaire_pid')}} hub_pid USING (pid_hk)
    INNER JOIN {{ref('latest_sat_openaire_researchproduct')}} sat_rp USING (researchproduct_hk)
    LEFT JOIN {{ref('link_openaire_researchproduct_author_rank')}} link_rp_author USING (researchproduct_hk)
    LEFT JOIN {{ref('hub_openaire_author_rank')}} hub_author USING (author_rank_hk)
    WHERE hub_pid.scheme = 'doi'
    GROUP BY 1,2,3,4,5
)

SELECT * FROM base
