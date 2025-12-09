WITH base AS (
    SELECT 
        lnk_r_p.record_hk,
        lnk_r_p.publisher_hk,
        lnk_r_p.record_publisher_hk
    FROM {{ ref('link_oai_record_publisher') }} lnk_r_p
)

SELECT * FROM base
