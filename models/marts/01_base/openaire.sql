WITH base AS (
    SELECT 
        hub_rp.researchproduct_id as id,
        sat_rp.main_title as title,
        sat_rp.type,
        sat_rp.publication_date as date,
        hub_rp.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp 
    INNER JOIN {{ref('latest_sat_openaire_researchproduct')}} sat_rp USING (researchproduct_hk)
),

rp_with_doi AS (
    SELECT 
        base.id,
        base.title,
        base.type,
        base.date,
        hub_pid.value as doi,
        base.researchproduct_hk
    FROM base
    INNER JOIN {{ref('link_openaire_researchproduct_pid')}} link_rp_pid USING (researchproduct_hk)
    LEFT JOIN {{ref('hub_openaire_pid')}} hub_pid USING (pid_hk)
    WHERE hub_pid.scheme = 'doi'
    GROUP BY 1,2,3,4,5,6
),

rp_without_doi AS (
    SELECT 
        base.id,
        base.title,
        base.type,
        base.date,
        NULL as doi,
        base.researchproduct_hk
    FROM base
    WHERE base.id NOT IN (SELECT id FROM rp_with_doi)
),

openaire_rp AS (
    SELECT DISTINCT * FROM (
        SELECT id, title, type, date, doi, researchproduct_hk
        FROM rp_with_doi
        UNION 
        SELECT id, title, type, date, doi, researchproduct_hk
        FROM rp_without_doi
    )
),
final AS (
    SELECT 
        openaire_rp.id,
        openaire_rp.title,
        openaire_rp.type,
        openaire_rp.date,
        openaire_rp.doi,
        STRING_AGG(hub_author.full_name, '|' ORDER BY hub_author.rank) as author
    FROM openaire_rp
    LEFT JOIN {{ref('link_openaire_researchproduct_author_rank')}} link_rp_author USING (researchproduct_hk)
    LEFT JOIN {{ref('hub_openaire_author_rank')}} hub_author USING (author_rank_hk)
    GROUP BY 1,2,3,4,5
)

SELECT * FROM final
