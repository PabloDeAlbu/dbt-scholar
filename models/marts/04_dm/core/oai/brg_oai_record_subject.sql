WITH base AS (
    SELECT 
        lnk_r_s.record_hk,
        lnk_r_s.subject_hk,
        lnk_r_s.record_subject_hk
    FROM {{ ref('link_oai_record_subject') }} lnk_r_s
)

SELECT * FROM base
