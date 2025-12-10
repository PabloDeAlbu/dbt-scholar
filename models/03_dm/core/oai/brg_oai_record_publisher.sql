WITH base AS (
    SELECT 
        hub_publisher.dc_publisher,
        lnk_r_p.record_hk,
        lnk_r_p.dc_publisher_hk,
        lnk_r_p.record_publisher_hk
    FROM {{ ref('link_oai_record_publisher') }} lnk_r_p
    JOIN {{ ref('hub_oai_publisher') }} hub_publisher USING (dc_publisher_hk)
)

SELECT * FROM base
