WITH base AS (
    SELECT 
        hub_subject.dc_subject,
        lnk_r_s.record_hk,
        lnk_r_s.dc_subject_hk,
        lnk_r_s.record_subject_hk
    FROM {{ ref('link_oai_record_subject') }} lnk_r_s
    JOIN {{ ref('hub_oai_subject') }} hub_subject USING(dc_subject_hk)
)

SELECT * FROM base
