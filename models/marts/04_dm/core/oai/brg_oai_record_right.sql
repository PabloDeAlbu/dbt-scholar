WITH base AS (
    SELECT 
        hub_right.dc_right,
        lnk_r_r.record_hk,
        lnk_r_r.dc_right_hk,
        lnk_r_r.record_right_hk
    FROM {{ ref('link_oai_record_right') }} lnk_r_r
    JOIN {{ ref('hub_oai_right') }} hub_right USING (dc_right_hk)    
)

SELECT * FROM base
