WITH base AS (
    SELECT 
        lnk_r_s.record_hk,
        lnk_r_s.set_hk,
        lnk_r_s.record_set_hk
    FROM {{ ref('link_oai_record_set') }} lnk_r_s
)

SELECT * FROM base
