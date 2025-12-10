WITH base AS (
    SELECT 
        hub_relation.dc_relation,
        lnk_r_r.record_hk,
        lnk_r_r.dc_relation_hk,
        lnk_r_r.record_relation_hk
    FROM {{ ref('link_oai_record_relation') }} lnk_r_r
    JOIN {{ ref('hub_oai_relation') }} hub_relation USING (dc_relation_hk)
)

SELECT * FROM base
