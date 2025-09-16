WITH base AS (
    SELECT 
        hub_work.work_id,
        hub_i.institution_id,
        sat_i.display_name,
        link_w_i.work_hk,
        link_w_i.institution_hk,
        link_w_i.work_institution_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_institution')}} link_w_i USING (work_hk)
    INNER JOIN {{ref('hub_openalex_institution')}} hub_i USING (institution_hk)
    INNER JOIN {{ref('sat_openalex_institution')}} sat_i USING (institution_hk)
)

SELECT * FROM base
