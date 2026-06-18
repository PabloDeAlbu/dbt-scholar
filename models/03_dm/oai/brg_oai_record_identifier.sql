WITH base AS (
    SELECT 
        hub_identifier.dc_identifier,
        lnk_r_i.record_hk,
        lnk_r_i.dc_identifier_hk,
        lnk_r_i.record_identifier_hk
    FROM {{ ref('link_oai_record_identifier') }} lnk_r_i
    JOIN {{ ref('hub_oai_identifier') }} hub_identifier USING (dc_identifier_hk)
)

SELECT * FROM base
