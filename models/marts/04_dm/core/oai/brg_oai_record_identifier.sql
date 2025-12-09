WITH base AS (
    SELECT 
        lnk_r_i.record_hk,
        lnk_r_i.identifier_hk,
        lnk_r_i.record_identifier_hk
    FROM {{ ref('link_oai_record_identifier') }} lnk_r_i
)

SELECT * FROM base
