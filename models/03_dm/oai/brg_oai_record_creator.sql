WITH base AS (
    SELECT 
        hub_creator.dc_creator,
        lnk_r_i.record_hk,
        lnk_r_i.dc_creator_hk,
        lnk_r_i.record_creator_hk
    FROM {{ ref('link_oai_record_creator') }} lnk_r_i
    JOIN {{ ref('hub_oai_creator') }} hub_creator USING (dc_creator_hk)
)

SELECT * FROM base
