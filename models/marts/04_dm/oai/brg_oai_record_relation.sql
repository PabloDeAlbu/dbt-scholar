WITH base AS (
    SELECT 
        lnk_r_r.record_hk,
        lnk_r_r.identifier_hk,
        lnk_r_r.record_identifier_hk
    FROM {{ ref('link_oai_record_identifier') }} lnk_r_r
)

SELECT * FROM base
