WITH base AS (
    SELECT 
        lnk_r_r.record_hk,
        lnk_r_r.right_hk,
        lnk_r_r.record_right_hk
    FROM {{ ref('link_oai_record_right') }} lnk_r_r
)

SELECT * FROM base
