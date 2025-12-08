WITH base AS (
    SELECT 
        lnk_r_l.record_hk,
        lnk_r_l.language_hk,
        lnk_r_l.record_language_hk
    FROM {{ ref('link_oai_record_language') }} lnk_r_l
)

SELECT * FROM base
