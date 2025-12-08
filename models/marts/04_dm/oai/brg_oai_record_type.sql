WITH base AS (
    SELECT 
        lnk_r_t.record_hk,
        lnk_r_t.type_hk,
        lnk_r_t.record_type_hk
    FROM {{ ref('link_oai_record_type') }} lnk_r_t
)

SELECT * FROM base
