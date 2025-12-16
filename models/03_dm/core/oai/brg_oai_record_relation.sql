WITH base AS (
    SELECT 
        hub_relation.dc_relation,
        split_part(hub_relation.dc_relation, '/', 3) AS relation_type,
        split_part(hub_relation.dc_relation, '/', 4) AS identifier_type,
        split_part(hub_relation.dc_relation, '/', 5) AS identifier_value,
        lnk_r_r.record_hk,
        lnk_r_r.dc_relation_hk,
        lnk_r_r.record_relation_hk
    FROM {{ ref('link_oai_record_relation') }} lnk_r_r
    JOIN {{ ref('hub_oai_relation') }} hub_relation USING (dc_relation_hk)
),

final AS (
    SELECT * FROM base
)

SELECT * FROM final
