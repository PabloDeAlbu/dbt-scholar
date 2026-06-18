WITH base AS (
    SELECT 
        hub_type.dc_type,
        lnk_r_t.record_hk,
        lnk_r_t.dc_type_hk,
        lnk_r_t.record_type_hk
    FROM {{ ref('link_oai_record_type') }} lnk_r_t
    JOIN {{ ref('hub_oai_type') }} hub_type USING(dc_type_hk)
)

SELECT * FROM base
