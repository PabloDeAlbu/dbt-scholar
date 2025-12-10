WITH base AS (
    SELECT 
        hub_language.dc_language,
        lnk_r_l.record_hk,
        lnk_r_l.dc_language_hk,
        lnk_r_l.record_language_hk
    FROM {{ ref('link_oai_record_language') }} lnk_r_l
    JOIN {{ ref('hub_oai_language') }} hub_language USING (dc_language_hk)
)

SELECT * FROM base
